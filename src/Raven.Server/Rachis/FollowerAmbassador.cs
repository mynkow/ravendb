﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Threading;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public enum AmbassadorStatus
    {
        None,
        Started,
        Connected,
        FailedToConnect,
        Disconnected,
        Closed,
    }

    public class FollowerAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Leader _leader;
        private ManualResetEvent _wakeLeader;
        private readonly string _tag;
        private readonly string _url;
        private readonly X509Certificate2 _certificate;
        private string _statusMessage;

        public string StatusMessage
        {
            get => _statusMessage;
            set
            {
                if (_statusMessage == value)
                    return;
                
                _statusMessage = value;
                _engine.NotifyTopologyChange();
            }
        }
        public AmbassadorStatus Status;

        private long _followerMatchIndex;
        private long _lastReplyFromFollower;
        private long _lastSendToFollower;
        private string _lastSentMsg;
        private PoolOfThreads.LongRunningWork _followerAmbassadorLongRunningOperation;
        private RemoteConnection _connection;
        private readonly MultipleUseFlag _running = new MultipleUseFlag(true);
        private readonly long _term;

        public string Tag => _tag;

        public string ThreadStatus
        {
            get
            {
                if (_followerAmbassadorLongRunningOperation == null)
                    return "Did not start";
                if (_followerAmbassadorLongRunningOperation.Join(0))
                    return "Finished";
                return "Running";
            }
        }

        public long FollowerMatchIndex => Interlocked.Read(ref _followerMatchIndex);

        public DateTime LastReplyFromFollower => new DateTime(Interlocked.Read(ref _lastReplyFromFollower));
        public DateTime LastSendToFollower => new DateTime(Interlocked.Read(ref _lastSendToFollower));
        public string LastSendMsg => _lastSentMsg;
        public bool ForceElectionsNow { get; set; }
        public string Url => _url;
        
        private readonly string _debugName;
        private readonly RachisLogRecorder _debugRecorder;

        private void UpdateLastSend(string msg)
        {
            Interlocked.Exchange(ref _lastSendToFollower, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _lastSentMsg, msg);
        }

        private void UpdateLastMatchFromFollower(long newVal)
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _followerMatchIndex, newVal);
            _wakeLeader.Set();
        }

        private void UpdateFollowerTicks()
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
        }
        
        public FollowerAmbassador(RachisConsensus engine, Leader leader, ManualResetEvent wakeLeader, string tag, string url, X509Certificate2 certificate, RemoteConnection connection = null)
        {
            _engine = engine;
            _term = leader.Term;
            _leader = leader;
            _wakeLeader = wakeLeader;
            _tag = tag;
            _url = url;
            _certificate = certificate;
            _connection = connection;
            Status = AmbassadorStatus.Started;
            StatusMessage = $"Started Follower Ambassador for {_engine.Tag} > {_tag} in term {_term}";
            
            _debugName = $"Follower Ambassador for {_tag} in term {_term}";
            _debugRecorder = _engine.InMemoryDebug.GetNewRecorder(_debugName);
        }
        
        public void UpdateLeaderWake(ManualResetEvent wakeLeader)
        {
            _wakeLeader = wakeLeader;
        }

        /// <summary>
        /// This method is expected to run for a long time (as long as we are the leader)
        /// it is responsible for talking to the remote follower and maintaining its state.
        /// This can never throw, and will run on its own thread.
        /// </summary>
        private unsafe void Run()
        {
            try
            {
                var needNewConnection = _connection == null;
                while (_leader.Running && _running)
                {
                    _engine.ValidateTerm(_term);
                    _debugRecorder.Start();
                    try
                    {
                        try
                        {
                            if (needNewConnection)
                            {
                                if (_engine.Log.IsInfoEnabled)
                                {
                                    _engine.Log.Info($"FollowerAmbassador for {_tag}: Creating new connection to {_tag}");
                                }
                                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                                {
                                    _connection?.Dispose();
                                    var (stream, disconnect) = _engine.ConnectToPeer(_url, _certificate, context).Result;
                                    var con = new RemoteConnection(_tag, _engine.Tag, _term, stream, disconnect);
                                    Interlocked.Exchange(ref _connection, con);
                                    ClusterTopology topology;
                                    using (context.OpenReadTransaction())
                                    {
                                        topology = _engine.GetTopology(context);
                                    }
                                    SendHello(context, topology);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Status = AmbassadorStatus.FailedToConnect;
                            StatusMessage = $"Failed to connect with {_tag}.{Environment.NewLine}" + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"{ToString()}: Failed to connect to remote follower: {_tag} {_url}", e);
                            }
                            // wait a bit
                            _leader.WaitForNewEntries().Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 2));
                            continue; // we'll retry connecting
                        }
                        finally
                        {
                            needNewConnection = true;
                            _debugRecorder.Record("Connection obtatined");
                        }

                        Status = AmbassadorStatus.Connected;
                        StatusMessage = $"Connected with {_tag}";

                        try
                        {
                            _engine.AppendStateDisposable(_leader, _connection);
                        }
                        catch (ConcurrencyException)
                        {
                            // we are no longer the leader, but we'll not abort the thread here, we'll 
                            // go to the top of the while loop and exit from there if needed
                            continue;
                        }

                        var matchIndex = InitialNegotiationWithFollower();
                        _debugRecorder.Record("start negotiation with follower");
                        UpdateLastMatchFromFollower(matchIndex);
                        SendSnapshot(_connection.Stream);
                        _debugRecorder.Record("Send snapshot");

                        var entries = new List<BlittableJsonReaderObject>();
                        var readWatcher = Stopwatch.StartNew();
                        while (_leader.Running && _running)
                        {
                            entries.Clear();
                            _engine.ValidateTerm(_term);
                            
                            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            {
                                AppendEntries appendEntries;
                                using (context.OpenReadTransaction())
                                {
                                    var table = context.Transaction.InnerTransaction.OpenTable(RachisConsensus.LogsTable, RachisConsensus.EntriesSlice);

                                    var reveredNextIndex = Bits.SwapBytes(_followerMatchIndex + 1);
                                    using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long), out Slice key))
                                    {
                                        long totalSize = 0;
                                        foreach (var value in table.SeekByPrimaryKey(key, 0))
                                        {
                                            var entry = BuildRachisEntryToSend(context, value);
                                            entries.Add(entry);
                                            totalSize += entry.Size;
                                            if (totalSize > Constants.Size.Megabyte)
                                                break;
                                        }

                                        appendEntries = new AppendEntries
                                        {
                                            ForceElections = ForceElectionsNow,
                                            EntriesCount = entries.Count,
                                            LeaderCommit = _engine.GetLastCommitIndex(context),
                                            Term = _term,
                                            TruncateLogBefore = _leader.LowestIndexInEntireCluster,
                                            PrevLogTerm = _engine.GetTermFor(context, _followerMatchIndex) ?? 0,
                                            PrevLogIndex = _followerMatchIndex,
                                            TimeAsLeader = _leader.LeaderShipDuration
                                        };
                                    }
                                }

                                // out of the tx, we can do network calls
                                UpdateLastSend(
                                    entries.Count > 0
                                        ? "Append Entries"
                                        : "Heartbeat"
                                );

                                if (_engine.Log.IsInfoEnabled && entries.Count > 0)
                                {
                                    _engine.Log.Info($"FollowerAmbassador for {_tag}: sending {entries.Count} entries to {_tag}"
#if DEBUG
                                                     + $" [{string.Join(" ,", entries.Select(x => x.ToString()))}]"
#endif
                                    );
                                }
                                _debugRecorder.Record("Sending entries");
                                _connection.Send(context, UpdateFollowerTicks, appendEntries, entries);
                                _debugRecorder.Record("Waiting for response");
                                AppendEntriesResponse aer = null;
                                while (true)
                                {
                                    readWatcher.Restart();
                                    try
                                    {
                                        aer = _connection.Read<AppendEntriesResponse>(context);
                                    }
                                    finally
                                    {
                                        if (readWatcher.Elapsed > _engine.ElectionTimeout / 2)
                                        {
                                            if (_engine.Log.IsInfoEnabled)
                                            {
                                                var msg = aer?.Success == true ? "successfully" : "failed";
                                                _engine.Log.Info($"{ToString()}: waited long time ({readWatcher.ElapsedMilliseconds}) to read a single response from stream ({msg}).");
                                            }
                                        }
                                    }
                                    
                                    if (aer.Pending == false)
                                        break;
                                    UpdateFollowerTicks();
                                }
                                _debugRecorder.Record("Response was recieved");
                                if (aer.Success == false)
                                {
                                    // shouldn't happen, the connection should be aborted if this is the case, but still
                                    var msg =
                                        "A negative Append Entries Response after the connection has been established shouldn't happen. Message: " +
                                        aer.Message;
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"{ToString()}: failure to append entries to {_tag} because: " + msg);
                                    }
                                    throw new InvalidOperationException(msg);
                                }
                                if (aer.CurrentTerm != _term)
                                    ThrowInvalidTermChanged(aer);

                                UpdateLastMatchFromFollower(aer.LastLogIndex);
                            }
                            
                            if(_running == false)
                                break;
                            
                            var task = _leader.WaitForNewEntries();
                            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            using (context.OpenReadTransaction())
                            {
                                if (_engine.GetLastEntryIndex(context) != _followerMatchIndex)
                                    continue; // instead of waiting, we have new entries, start immediately
                            }
                            
                            // either we have new entries to send, or we waited for long enough 
                            // to send another heartbeat
                            task.Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 3));
                            UpdateFollowerTicks(); // keep the leader in full confidence of his leadership 
                            _debugRecorder.Record("Cycle done");
                            _debugRecorder.Start();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        Status = AmbassadorStatus.FailedToConnect;
                        StatusMessage = $"Failed to talk with {_tag}.{Environment.NewLine}" + e;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info("Failed to talk to remote follower: " + _tag, e);
                        }
                        // notify leader about an error

                        _connection?.Dispose();

                        _leader?.NotifyAboutException(Tag, e);
                        _leader.WaitForNewEntries().Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 2));
                    }
                    finally
                    {
                        if (Status == AmbassadorStatus.Connected)
                        {
                            StatusMessage = "Disconnected";
                        }
                        else
                        {
                            StatusMessage = "Disconnected due to :" + StatusMessage;
                        }
                        Status = AmbassadorStatus.Disconnected;
                        _debugRecorder.Record(StatusMessage);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                StatusMessage = "Closed";
                Status = AmbassadorStatus.Closed;
            }
            catch (ObjectDisposedException)
            {
                StatusMessage = "Closed";
                Status = AmbassadorStatus.Closed;
            }
            catch (AggregateException ae)
                when (ae.InnerException is OperationCanceledException || ae.InnerException is ObjectDisposedException)
            {
                StatusMessage = "Closed";
                Status = AmbassadorStatus.Closed;
            }
            catch (Exception e)
            {
                StatusMessage = $"Failed to talk with {_tag}.{Environment.NewLine}" + e.Message;
                Status = AmbassadorStatus.FailedToConnect;

                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote follower: " + _tag, e);
                }
            }
            finally
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Node {_tag} is finished with the message '{StatusMessage}'.");
                }
                _connection?.Dispose();
            }
        }

        private void ThrowInvalidTermChanged(AppendEntriesResponse aer)
        {
            throw new ConcurrencyException($"The current engine term has changed " +
                                           $"({aer.CurrentTerm:#,#;;0} -> {_term:#,#;;0}), " +
                                           $"this ambassador term is no longer valid");
        }

        private void SendSnapshot(Stream stream)
        {
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var earliestIndexEtry = _engine.GetFirstEntryIndex(context);
                if (_followerMatchIndex >= earliestIndexEtry)
                {
                    // we don't need a snapshot, so just send updated topology
                    UpdateLastSend("Send empty snapshot");
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: sending empty snapshot to {_tag}");
                    }
                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = earliestIndexEtry,
                        LastIncludedTerm = _engine.GetTermForKnownExisting(context, earliestIndexEtry),
                        Topology = _engine.GetTopologyRaw(context)
                    });
                    using (var binaryWriter = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
                    {
                        binaryWriter.Write(-1);
                    }
                }
                else
                {
                    _engine.GetLastCommitIndex(context, out long index, out long term);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: sending snapshot to {_tag} with index={index} term={term}");
                    }
                    // we make sure that we routinely update LastReplyFromFollower here
                    // so we'll not leave the leader thinking we abandoned it
                    UpdateLastMatchFromFollower(_followerMatchIndex);
                    UpdateLastSend("Send full snapshot");
                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = index,
                        LastIncludedTerm = term,
                        Topology = _engine.GetTopologyRaw(context)
                    });
                    WriteSnapshotToFile(context, new BufferedStream(stream));

                    UpdateLastMatchFromFollower(_followerMatchIndex);
                }

                while (true)
                {
                    var aer = _connection.Read<InstallSnapshotResponse>(context);
                    if (aer.Done)
                    {
                        UpdateLastMatchFromFollower(aer.LastLogIndex);
                        break;
                    }
                    UpdateLastMatchFromFollower(_followerMatchIndex);
                }

                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: done sending snapshot to {_tag}");
                }
            }
        }

        private void MaybeNotifyLeaderThatWeAreSillAlive(long count, Stopwatch sp)
        {
            if (count % 100 != 0)
                return;

            if (sp.ElapsedMilliseconds <= _engine.ElectionTimeout.TotalMilliseconds / 2)
                return;

            sp.Restart();

            UpdateLastMatchFromFollower(_followerMatchIndex);
        }

        private unsafe void WriteSnapshotToFile(TransactionOperationContext context, Stream dest)
        {
            var copier = new UnmanagedMemoryToStream(dest);
            var sp = Stopwatch.StartNew();
            long count = 0;

            using (var binaryWriter = new BinaryWriter(dest, Encoding.UTF8, leaveOpen: true))
            {
                var txr = context.Transaction.InnerTransaction;
                var llt = txr.LowLevelTransaction;
                using (var rootIterator = llt.RootObjects.Iterate(false))
                {
                    if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                        throw new InvalidOperationException("Root objects iterations must always have _something_!");
                    do
                    {
                        var rootObjectType = txr.GetRootObjectType(rootIterator.CurrentKey);
                        if (_engine.ShouldSnapshot(rootIterator.CurrentKey, rootObjectType) == false)
                            continue;

                        MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);

                        var currentTreeKey = rootIterator.CurrentKey;

                        binaryWriter.Write((int)rootObjectType);
                        binaryWriter.Write(currentTreeKey.Size);
                        copier.Copy(currentTreeKey.Content.Ptr, currentTreeKey.Size);

                        switch (rootObjectType)
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = txr.ReadTree(currentTreeKey);
                                binaryWriter.Write(tree.State.NumberOfEntries);

                                using (var treeIterator = tree.Iterate(false))
                                {
                                    if (treeIterator.Seek(Slices.BeforeAllKeys))
                                    {
                                        do
                                        {
                                            var currentTreeValueKey = treeIterator.CurrentKey;
                                            binaryWriter.Write(currentTreeValueKey.Size);
                                            copier.Copy(currentTreeValueKey.Content.Ptr, currentTreeValueKey.Size);
                                            var reader = treeIterator.CreateReaderForCurrent();
                                            binaryWriter.Write(reader.Length);
                                            copier.Copy(reader.Base, reader.Length);
                                            MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);
                                        } while (treeIterator.MoveNext());
                                    }
                                }
                                break;
                            case RootObjectType.Table:
                                var tableTree = txr.ReadTree(currentTreeKey, RootObjectType.Table);

                                // Get the table schema
                                var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                                var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                                var schema = TableSchema.ReadFrom(txr.Allocator, schemaPtr, schemaSize);

                                // Load table into structure 
                                var inputTable = txr.OpenTable(schema, currentTreeKey);
                                binaryWriter.Write(inputTable.NumberOfEntries);
                                foreach (var holder in inputTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                                {
                                    MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);
                                    binaryWriter.Write(holder.Reader.Size);
                                    copier.Copy(holder.Reader.Pointer, holder.Reader.Size);
                                }
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(rootObjectType), rootObjectType + " " + rootIterator.CurrentKey);
                        }

                    } while (rootIterator.MoveNext());
                }
                binaryWriter.Write((int)RootObjectType.None);
            }
            MaybeNotifyLeaderThatWeAreSillAlive(0, sp);

            dest.Flush();
        }

        private unsafe class UnmanagedMemoryToStream
        {
            private readonly byte[] _buffer = new byte[1024];

            private readonly Stream _stream;

            public UnmanagedMemoryToStream(Stream stream)
            {
                _stream = stream;
            }

            public void Copy(byte* ptr, int size)
            {
                fixed (byte* pBuffer = _buffer)
                {
                    while (size > 0)
                    {
                        var count = Math.Min(size, _buffer.Length);
                        Memory.Copy(pBuffer, ptr, count);
                        _stream.Write(_buffer, 0, count);
                        ptr += count;
                        size -= count;
                    }
                }
            }
        }


        internal static unsafe BlittableJsonReaderObject BuildRachisEntryToSend(TransactionOperationContext context,
            Table.TableValueHolder value)
        {
            BlittableJsonReaderObject entry;
            using (var writer =
                new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(
                    context, BlittableJsonDocumentBuilder.UsageMode.None))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                writer.StartWriteObjectDocument();
                writer.StartWriteObject();

                writer.WritePropertyName("Type");
                writer.WriteValue(nameof(RachisEntry));

                writer.WritePropertyName(nameof(RachisEntry.Index));

                var index = Bits.SwapBytes(*(long*)value.Reader.Read(0, out int size));
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(index);

                writer.WritePropertyName(nameof(RachisEntry.Term));
                var term = *(long*)value.Reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(term);

                writer.WritePropertyName(nameof(RachisEntry.Entry));
                writer.WriteEmbeddedBlittableDocument(value.Reader.Read(2, out size), size);


                writer.WritePropertyName(nameof(RachisEntry.Flags));
                var flags = *(RachisEntryFlags*)value.Reader.Read(3, out size);
                Debug.Assert(size == sizeof(RachisEntryFlags));
                writer.WriteValue(flags.ToString());


                writer.WriteObjectEnd();
                writer.FinalizeDocument();
                entry = writer.CreateReader();
            }
            return entry;
        }

        private long InitialNegotiationWithFollower()
        {
            UpdateLastMatchFromFollower(0);
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                LogLengthNegotiation lln;
                using (context.OpenReadTransaction())
                {
                    var lastIndexEntry = _engine.GetLastEntryIndex(context);
                    lln = new LogLengthNegotiation
                    {
                        Term = _term,
                        PrevLogIndex = lastIndexEntry,
                        PrevLogTerm = _engine.GetTermForKnownExisting(context, lastIndexEntry)
                    };
                }

                UpdateLastSend("Negotiation");
                _connection.Send(context, lln);

                var llr = _connection.Read<LogLengthNegotiationResponse>(context);
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Got 1st LogLengthNegotiationResponse from {_tag} with term {llr.CurrentTerm:#,#;;0} " +
                                     $"({llr.MidpointIndex:#,#;;0} / {llr.MidpointTerm:#,#;;0}) {llr.Status}");
                }
                // need to negotiate
                do
                {
                    if (llr.CurrentTerm > _term)
                    {
                        // we need to abort the current leadership
                        var msg = $"{ToString()}: found election term {llr.CurrentTerm:#,#;;0} that is higher than ours {_term:#,#;;0}";
                        _engine.SetNewState(RachisState.Follower, null, _term, msg);
                        _engine.FoundAboutHigherTerm(llr.CurrentTerm, "Append entries response with higher term");
                        throw new InvalidOperationException(msg);
                    }

                    if (llr.Status == LogLengthNegotiationResponse.ResponseStatus.Acceptable)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: {_tag} agreed on term={llr.CurrentTerm:#,#;;0} index={llr.LastLogIndex:#,#;;0}");
                        }
                        return llr.LastLogIndex;
                    }

                    if (llr.Status == LogLengthNegotiationResponse.ResponseStatus.Rejected)
                    {
                        var message = "Failed to get acceptable status from " + _tag + " because " + llr.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: {message}");
                        }
                        throw new InvalidOperationException(message);
                    }

                    UpdateLastMatchFromFollower(0);

                    using (context.OpenReadTransaction())
                    {
                        var termForMidpointIndex = _engine.GetTermFor(context, llr.MidpointIndex);
                        bool truncated = false;
                        if (termForMidpointIndex == null) //follower has this log entry but we already truncated it.
                        {
                            truncated = true;
                        }
                        else if (llr.MidpointTerm == termForMidpointIndex)
                        {
                            llr.MinIndex = Math.Min(llr.MidpointIndex + 1, llr.MaxIndex);
                        }
                        else
                        {
                            llr.MaxIndex = Math.Max(llr.MidpointIndex - 1, llr.MinIndex);
                        }
                        var midIndex = (llr.MinIndex + llr.MaxIndex) / 2;
                        var termFor = _engine.GetTermFor(context, midIndex);
                        Debug.Assert(termFor != 0);
                        lln = new LogLengthNegotiation
                        {
                            Term = _term,
                            PrevLogIndex = midIndex,
                            PrevLogTerm = termFor ?? 0,
                            Truncated = truncated || termFor == null
                        };
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Sending LogLengthNegotiation to {_tag} with term {lln.Term:#,#;;0} " +
                                             $"({lln.PrevLogIndex:#,#;;0} / {lln.PrevLogTerm:#,#;;0}) - Trnuncated {lln.Truncated}");
                        }
                    }
                    UpdateLastSend("Negotiation 2");
                    _connection.Send(context, lln);
                    llr = _connection.Read<LogLengthNegotiationResponse>(context);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Got LogLengthNegotiationResponse from {_tag} with term {llr.CurrentTerm} " +
                                         $"({llr.MidpointIndex:#,#;;0} / {llr.MidpointTerm:#,#;;0}) {llr.Status}");
                    }
                } while (true);
            }
        }

        private void SendHello(TransactionOperationContext context, ClusterTopology clusterTopology)
        {
            UpdateLastSend("Hello");
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: sending Rachis hello to {_tag}");
            }
            _connection.Send(context, new RachisHello
            {
                TopologyId = clusterTopology.TopologyId,
                InitialMessageType = InitialMessageType.AppendEntries,
                DebugDestinationIdentifier = _tag,
                DebugSourceIdentifier = _engine.Tag,
                ElectionTimeout = (int)_engine.ElectionTimeout.TotalMilliseconds,
                SendingThread = Thread.CurrentThread.ManagedThreadId,
                DestinationUrl = _url,
                SourceUrl = _engine.Url
            });
        }

        public void Start()
        {
            UpdateLastMatchFromFollower(0);
            _followerAmbassadorLongRunningOperation =
                PoolOfThreads.GlobalRavenThreadPool.Value.LongRunning(x => Run(), null, ToString());                
        }

        public override string ToString()
        {
            return $"Follower Ambassador for {_tag} in term {_term:#,#;;0}";
        }

        public void Dispose()
        {
            _running.Lower();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Dispose {ToString()}");
            }
            if (_followerAmbassadorLongRunningOperation != null && _followerAmbassadorLongRunningOperation.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
            {
                Volatile.Read(ref _connection)?.Dispose();

                while (_followerAmbassadorLongRunningOperation.Join(1000) == false)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: Waited for a full second for thread {_followerAmbassadorLongRunningOperation.ManagedThreadId} ({(_followerAmbassadorLongRunningOperation.Join(0)?"Running":"Finished")}) to close, disposing connection and trying");
                    }
                    // the thread may have create a new connection, so need
                    // to dispose that as well

                    Volatile.Read(ref _connection)?.Dispose();
                }
            }
            _engine.InMemoryDebug.RemoveRecorder(_debugName);
        }
    }
}
