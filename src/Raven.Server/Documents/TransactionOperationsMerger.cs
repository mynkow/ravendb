﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Global;
using static Sparrow.DatabasePerformanceMetrics;

namespace Raven.Server.Documents
{
    /// <summary>
    /// Merges multiple commands into a single transaction. Any commands that implement IDisposable
    /// will be disposed after the command is executed and transaction is committed
    /// </summary>
    public class TransactionOperationsMerger : IDisposable
    {
        private readonly DocumentDatabase _parent;
        private readonly CancellationToken _shutdown;
        private bool _runTransactions = true;
        private readonly ConcurrentQueue<MergedTransactionCommand> _operations = new ConcurrentQueue<MergedTransactionCommand>();
        private readonly CountdownEvent _concurrentOperations = new CountdownEvent(1);

        private readonly ConcurrentQueue<List<MergedTransactionCommand>> _opsBuffers = new ConcurrentQueue<List<MergedTransactionCommand>>();
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        private ExceptionDispatchInfo _edi;
        private readonly Logger _log;
        private PoolOfThreads.LongRunningWork _txLongRunningOperation;

        public TransactionOperationsMerger(DocumentDatabase parent, CancellationToken shutdown)
        {
            _parent = parent;
            _log = LoggingSource.Instance.GetLogger<TransactionOperationsMerger>(_parent.Name);
            _shutdown = shutdown;
        }

        public DatabasePerformanceMetrics GeneralWaitPerformanceMetrics = new DatabasePerformanceMetrics(MetricType.GeneralWait, 256, 1);
        public DatabasePerformanceMetrics TransactionPerformanceMetrics = new DatabasePerformanceMetrics(MetricType.Transaction, 256, 8);


        private string TransactionMergerThreadName => _parent.Name + " transaction merging thread";

        public void Start()
        {
            _txLongRunningOperation = PoolOfThreads.GlobalRavenThreadPool.Value.LongRunning(x => MergeOperationThreadProc(), null, TransactionMergerThreadName);            
        }

        public abstract class MergedTransactionCommand
        {
            public abstract int Execute(DocumentsOperationContext context);
            public readonly TaskCompletionSource<object> TaskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            public Exception Exception;
        }

        /// <summary>
        /// Enqueue the command to be eventually executed. If the command implements
        ///  IDisposable, the command will be disposed after it is run and a tx is committed.
        /// </summary>
        public async Task Enqueue(MergedTransactionCommand cmd)
        {
            _edi?.Throw();

            _operations.Enqueue(cmd);
            _waitHandle.Set();

            if (_concurrentOperations.TryAddCount() == false)
                ThrowTxMergerWasDisposed();

            try
            {
                await cmd.TaskCompletionSource.Task;
            }
            finally
            {
                try
                {
                    _concurrentOperations.Signal(); // done with this
                }
                catch (InvalidOperationException)
                {
                    // Expected: "Invalid attempt made to decrement the event's count below zero."
                }
            }
        }

        private static void ThrowTxMergerWasDisposed()
        {
            throw new ObjectDisposedException("Transaction Merger");
        }

        private void MergeOperationThreadProc()
        {
            var oomTimer = new Stopwatch();// this is allocated here to avoid OOM when using it

            while (true) // this is actually only executed once, except if we are trying to recover from OOM errors
            {
                NativeMemory.EnsureRegistered();
                try
                {
                    while (_runTransactions)
                    {
                        if (_operations.IsEmpty)
                        {
                            using (var generalMeter = GeneralWaitPerformanceMetrics.MeterPerformanceRate())
                            {
                                generalMeter.IncrementCounter(1);
                                _waitHandle.Wait(_shutdown);
                            }
                            _waitHandle.Reset();
                        }

                        MergeTransactionsOnce();
                    }
                    return;
                }
                catch (OperationCanceledException)
                {
                    // clean shutdown, nothing to do
                }
                catch (Exception e) when (e is OutOfMemoryException)
                {
                    // this catch block is meant to handle potentially transient errors
                    // in particular, an OOM error is something that we want to recover
                    // we'll handle this by throwing out all the pending transactions,
                    // waiting for 3 seconds and then resuming normal operations
                    if (_log.IsOperationsEnabled)
                    {
                        try
                        {
                            _log.Operations(
                                "OutOfMemoryException happened in the transaction merger, will abort all transactions for the next 3 seconds and then resume operations",
                                e);
                        }
                        catch 
                        {
                            // under these conditions, this may throw, but we don't care, we wanna survive (cue music)
                        }
                    }
                    ClearQueueWithException(e);
                    oomTimer.Restart();
                    while (_runTransactions)
                    {
                        try
                        {
                            var timeSpan = TimeSpan.FromSeconds(3) - oomTimer.Elapsed;
                            if (timeSpan <= TimeSpan.Zero || _waitHandle.Wait(timeSpan, _shutdown) == false)
                                break;
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                        ClearQueueWithException(e);
                    }
                    oomTimer.Stop();
                    // and now we return to the top of the loop and restart
                }
                catch (Exception e)
                {
                    if (_log.IsOperationsEnabled)
                    {
                        _log.Operations(
                            "Serious failure in transaction merging thread, the database must be restarted!",
                            e);
                    }
                    Interlocked.Exchange(ref _edi, ExceptionDispatchInfo.Capture(e));
                    // cautionary, we make sure that stuff that is waiting on the 
                    // queue is notified about this catastrophic3 error and we wait
                    // just a bit more to verify that nothing racy can still get 
                    // there
                    while (_runTransactions)
                    {
                        ClearQueueWithException(e);
                        try
                        {
                            _waitHandle.Wait(_shutdown);
                            _waitHandle.Reset();
                        }
                        catch (ObjectDisposedException)
                        {
                            return;
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
            }

            void ClearQueueWithException(Exception e)
            {
                while (_operations.TryDequeue(out MergedTransactionCommand result))
                {
                    result.Exception = e;
                    NotifyOnThreadPool(result);
                }
            }
        }

        private List<MergedTransactionCommand> GetBufferForPendingOps()
        {
            if (_opsBuffers.TryDequeue(out var pendingOps) == false)
            {
                return new List<MergedTransactionCommand>();
            }
            return pendingOps;
        }

        private void DoCommandsNotification(object cmds)
        {
            var pendingOperations = (List<MergedTransactionCommand>)cmds;
            foreach (var op in pendingOperations)
            {
                DoCommandNotification(op);
            }
            pendingOperations.Clear();
            _opsBuffers.Enqueue(pendingOperations);
        }

        private void DoCommandNotification(object op)
        {
            DoCommandNotification((MergedTransactionCommand)op);
        }

        private static void DoCommandNotification(MergedTransactionCommand cmd)
        {
            if (cmd.Exception != null)
            {
                cmd.TaskCompletionSource.TrySetException(cmd.Exception);
            }
            else
            {
                cmd.TaskCompletionSource.TrySetResult(null);
            }

        }

        private void MergeTransactionsOnce()
        {
            DocumentsTransaction tx = null;
            try
            {
                var pendingOps = GetBufferForPendingOps();
                using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    try
                    {
                        tx = context.OpenWriteTransaction();
                    }
                    catch (Exception e)
                    {
                        if (_operations.TryDequeue(out MergedTransactionCommand command))
                        {
                            command.Exception = e;
                            DoCommandNotification(command);
                        }

                        return;
                    }
                    PendingOperations result;
                    try
                    {
                        var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                        try
                        {
                            result = ExecutePendingOperationsInTransaction(pendingOps, context, null, ref transactionMeter);
                            UpdateGlobalReplicationInfoBeforeCommit(context);
                        }
                        finally
                        {
                            transactionMeter.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info(
                                $"Failed to run merged transaction with {pendingOps.Count:#,#0}, will retry independently",
                                e);
                        }
                        tx.Dispose();
                        NotifyTransactionFailureAndRerunIndependently(pendingOps, e);
                        return;
                    }

                    switch (result)
                    {
                        case PendingOperations.CompletedAll:
                            try
                            {
                                tx.Commit();
                                tx.Dispose();
                            }
                            catch (Exception e)
                            {
                                foreach (var op in pendingOps)
                                {
                                    op.Exception = e;
                                }
                            }
                            finally
                            {
                                NotifyOnThreadPool(pendingOps);
                            }
                            return;
                        case PendingOperations.HasMore:
                            MergeTransactionsWithAsyncCommit(context, pendingOps);
                            return;
                        default:
                            Debug.Assert(false, "Should never happen");
                            return;
                    }
                }
            }
            finally
            {
                tx?.Dispose();
            }
        }

        private static void UpdateGlobalReplicationInfoBeforeCommit(DocumentsOperationContext context)
        {
            if (string.IsNullOrEmpty(context.LastDatabaseChangeVector) == false)
            {
                DocumentsStorage.SetDatabaseChangeVector(context, context.LastDatabaseChangeVector);
            }

            if (context.LastReplicationEtagFrom != null)
            {
                foreach (var repEtag in context.LastReplicationEtagFrom)
                {
                    DocumentsStorage.SetLastReplicatedEtagFrom(context, repEtag.Key, repEtag.Value);
                }
            }
        }

        private void NotifyTransactionFailureAndRerunIndependently(List<MergedTransactionCommand> pendingOps, Exception e)
        {
            if (pendingOps.Count == 1)
            {
                pendingOps[0].Exception = e;
                NotifyOnThreadPool(pendingOps);
                return;
            }
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Error when merging {0} transactions, will try running independently", e);
            }
            RunEachOperationIndependently(pendingOps);
        }

        private void MergeTransactionsWithAsyncCommit(
            DocumentsOperationContext context,
            List<MergedTransactionCommand> previousPendingOps)
        {
            var previous = context.Transaction;
            try
            {
                while (true)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"BeginAsyncCommit on {previous.InnerTransaction.LowLevelTransaction.Id} with {_operations.Count} additional operations pending");
                    try
                    {
                        context.Transaction = previous.BeginAsyncCommitAndStartNewTransaction();
                    }
                    catch (Exception e)
                    {
                        foreach (var op in previousPendingOps)
                        {
                            op.Exception = e;
                        }
                        NotifyOnThreadPool(previousPendingOps);
                        return;
                    }
                    try
                    {
                        var currentPendingOps = GetBufferForPendingOps();
                        PendingOperations result;
                        bool calledCompletePreviousTx = false;
                        try
                        {
                            var transactionMeter = TransactionPerformanceMetrics.MeterPerformanceRate();
                            try
                            {

                                result = ExecutePendingOperationsInTransaction(
                                    currentPendingOps, context,
                                    previous.InnerTransaction.LowLevelTransaction.AsyncCommit, ref transactionMeter);
                                UpdateGlobalReplicationInfoBeforeCommit(context);
                            }
                            finally
                            {
                                transactionMeter.Dispose();
                            }
                            calledCompletePreviousTx = true;
                            CompletePreviousTransaction(previous, ref previousPendingOps, throwOnError: true);
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info(
                                    $"Failed to run merged transaction with {currentPendingOps.Count:#,#0} operations in async manner, will retry independently",
                                    e);
                            }
                            using (previous)
                            using (context.Transaction)
                            {
                                if (calledCompletePreviousTx == false)
                                {
                                    CompletePreviousTransaction(previous, ref previousPendingOps,
                                        // if this previous threw, it won't throw again
                                        throwOnError: false);
                                }
                                else
                                {
                                    throw;
                                }
                            }
                            NotifyTransactionFailureAndRerunIndependently(currentPendingOps, e);
                            return;
                        }
                        previous.Dispose();

                        switch (result)
                        {
                            case PendingOperations.CompletedAll:
                                try
                                {
                                    context.Transaction.Commit();
                                    context.Transaction.Dispose();
                                }
                                catch (Exception e)
                                {
                                    foreach (var op in currentPendingOps)
                                    {
                                        op.Exception = e;
                                    }
                                }
                                NotifyOnThreadPool(currentPendingOps);
                                return;
                            case PendingOperations.HasMore:
                                previousPendingOps = currentPendingOps;
                                previous = context.Transaction;
                                context.Transaction = null;
                                break;
                            default:
                                Debug.Assert(false);
                                return;
                        }

                    }
                    finally
                    {
                        context.Transaction?.Dispose();
                    }
                }
            }
            finally
            {
                previous.Dispose();
            }
        }

        private void CompletePreviousTransaction(
            RavenTransaction previous,
            ref List<MergedTransactionCommand> previousPendingOps,
            bool throwOnError)
        {
            try
            {
                previous.EndAsyncCommit();
                if (_log.IsInfoEnabled)
                    _log.Info($"EndAsyncCommit on {previous.InnerTransaction.LowLevelTransaction.Id}");

                NotifyOnThreadPool(previousPendingOps);
            }
            catch (Exception e)
            {
                foreach (var op in previousPendingOps)
                {
                    op.Exception = e;
                }
                NotifyOnThreadPool(previousPendingOps);
                previousPendingOps = null; // RavenDB-7417
                if (throwOnError)
                    throw;
            }
        }

        private enum PendingOperations
        {
            CompletedAll,
            HasMore
        }

        const int MaxTimeToWait = 1000;

        private bool _alreadyListeningToPreviousOperationEnd;


        private PendingOperations ExecutePendingOperationsInTransaction(
            List<MergedTransactionCommand> pendingOps,
            DocumentsOperationContext context,
            Task previousOperation, ref PerformanceMetrics.DurationMeasurement meter)
        {
            _alreadyListeningToPreviousOperationEnd = false;
            var sp = Stopwatch.StartNew();
            do
            {
                // RavenDB-7732 - Even if we merged multiple seprate operations into 
                // a single transaction in Voron, we're still going to have a separate
                // tx marker for them for the purpose of replication, to avoid creating
                // overly large replication batches.
                context.TransactionMarkerOffset++;

                if (TryGetNextOperation(previousOperation, out MergedTransactionCommand op, ref meter) == false)
                    break;

                pendingOps.Add(op);
                meter.IncrementCounter(1);
                meter.IncreamentCommands(op.Execute(context));

                if (previousOperation != null && previousOperation.IsCompleted)
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Stopping merged operations because previous transaction async commit completed. Took {sp.Elapsed} with {pendingOps.Count} operations and {_operations.Count} remaining operations");
                    }
                    return GetPendingOperationsStatus(context);
                }
                if (sp.ElapsedMilliseconds > MaxTimeToWait)
                {
                    if (previousOperation != null)
                    {
                        continue;
                    }
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Stopping merged operations because {sp.Elapsed} passed {pendingOps.Count} operations and {_operations.Count} remaining operations");
                    }
                    return GetPendingOperationsStatus(context);
                }

                if (IntPtr.Size == sizeof(int) || _parent.Configuration.Storage.ForceUsing32BitsPager)
                {
                    // we need to be sure that we don't use up too much virtual space
                    var llt = context.Transaction.InnerTransaction.LowLevelTransaction;
                    var modifiedSize = llt.NumberOfModifiedPages * Constants.Storage.PageSize;
                    if (modifiedSize > 4 * Constants.Size.Megabyte)
                    {
                        return GetPendingOperationsStatus(context);
                    }
                }

            } while (true);
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Merged {pendingOps.Count} operations in {sp.Elapsed} and there is no more work");
            }

            return GetPendingOperationsStatus(context, pendingOps.Count == 0);
        }

        private bool TryGetNextOperation(Task previousOperation, out MergedTransactionCommand op, ref PerformanceMetrics.DurationMeasurement meter)
        {
            if (_operations.TryDequeue(out op))
                return true;

            if (previousOperation == null || previousOperation.IsCompleted)
                return false;

            return UnlikelyWaitForNextOperationOrPreviousTransactionComplete(previousOperation, out op, ref meter);
        }

        private bool UnlikelyWaitForNextOperationOrPreviousTransactionComplete(Task previousOperation,
            out MergedTransactionCommand op, ref PerformanceMetrics.DurationMeasurement meter)
        {
            if (_alreadyListeningToPreviousOperationEnd == false)
            {
                _alreadyListeningToPreviousOperationEnd = true;
                if (previousOperation.IsCompleted)
                    _waitHandle.Set();
                else
                    previousOperation.ContinueWith(_ => _waitHandle.Set(), _shutdown);
            }
            while (true)
            {
                try
                {
                    meter.MarkInternalWindowStart();
                    _waitHandle.Wait(_shutdown);
                    _waitHandle.Reset();
                    if (previousOperation.IsCompleted)
                    {
                        op = null;
                        return false;
                    }
                    if (_operations.TryDequeue(out op))
                        return true;
                }
                finally
                {
                    meter.MarkInternalWindowEnd();
                }
            }
        }

        private PendingOperations GetPendingOperationsStatus(DocumentsOperationContext context, bool forceCompletion = false)
        {
            // this optimization is disabled for 32 bits
            if (sizeof(int) == IntPtr.Size || _parent.Configuration.Storage.ForceUsing32BitsPager)
                return PendingOperations.CompletedAll;

            // This optimization is disabled when encryption is on	
            if (context.Environment.Options.EncryptionEnabled)
                return PendingOperations.CompletedAll;

            if (forceCompletion)
                return PendingOperations.CompletedAll;

            return PendingOperations.HasMore;
        }

        private void NotifyOnThreadPool(MergedTransactionCommand cmd)
        {
            TaskExecutor.Execute(DoCommandNotification, cmd);
        }


        private void NotifyOnThreadPool(List<MergedTransactionCommand> cmds)
        {
            if (cmds == null)
                return;
            TaskExecutor.Execute(DoCommandsNotification, cmds);
        }


        private void RunEachOperationIndependently(List<MergedTransactionCommand> pendingOps)
        {
            try
            {
                foreach (var op in pendingOps)
                {
                    try
                    {
                        using (_parent.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            using (var tx = context.OpenWriteTransaction())
                            {
                                op.Execute(context);
                                tx.Commit();
                            }
                        }
                        DoCommandNotification(op);
                    }
                    catch (Exception e)
                    {
                        op.Exception = e;
                        NotifyOnThreadPool(op);
                    }
                }
            }
            finally
            {
                pendingOps.Clear();
                _opsBuffers.Enqueue(pendingOps);
            }
        }

        public void Dispose()
        {
            _runTransactions = false;

            // once all the concurrent transactions are done, this will signal the event, preventing
            // it from adding additional operations
            var done = _concurrentOperations.Signal();

            _waitHandle.Set();
            _txLongRunningOperation?.Join(int.MaxValue);
            _waitHandle.Dispose();

            // make sure that the queue is empty and there are no pending 
            // transactions waiting. 
            // this is probably a bit more aggresive that what is needed, but it is better 
            // to be cautious and slower on rare dispose than hang

            while (done == false)
            {
                try
                {
                    done = _concurrentOperations.Signal();
                }
                catch (InvalidOperationException)
                {
                    break;
                }
            }

            while (_operations.TryDequeue(out MergedTransactionCommand result))
            {
                result.TaskCompletionSource.TrySetCanceled();
            }
        }
    }
}
