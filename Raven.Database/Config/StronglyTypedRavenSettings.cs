﻿// -----------------------------------------------------------------------
//  <copyright file="StronglyTypedRavenSettings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Runtime.Caching;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config
{
	public class StronglyTypedRavenSettings
	{
		private readonly NameValueCollection settings;

		public StronglyTypedRavenSettings(NameValueCollection settings)
		{
			this.settings = settings;
		}

		public void Setup(int defaultMaxNumberOfItemsToIndexInSingleBatch, int defaultInitialNumberOfItemsToIndexInSingleBatch)
		{
			MaxConcurrentServerRequests = new IntegerSetting(settings[Constants.MaxConcurrentServerRequests], 512);

            MaxConcurrentRequestsForDatabaseDuringLoad = new IntegerSetting(settings[Constants.MaxConcurrentRequestsForDatabaseDuringLoad], 10);

            MaxSecondsForTaskToWaitForDatabaseToLoad = new IntegerSetting(settings[Constants.MaxSecondsForTaskToWaitForDatabaseToLoad], 10);

			MaxConcurrentMultiGetRequests = new IntegerSetting(settings[Constants.MaxConcurrentMultiGetRequests], 192);

			MemoryLimitForIndexing = new IntegerSetting(settings[Constants.MemoryLimitForIndexing],
                // we allow 1 GB by default, or up to 75% of available memory on startup, if less than that is available
                Math.Min(1024, (int)(MemoryStatistics.AvailableMemory * 0.75)));


            int workerThreads;
            int completionThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionThreads);
            MinThreadPoolWorkerThreads =
                new IntegerSettingWithMin(settings["Raven/MinThreadPoolWorkerThreads"], workerThreads, 2);
            MinThreadPoolCompletionThreads =
                new IntegerSettingWithMin(settings["Raven/MinThreadPoolCompletionThreads"], completionThreads, 2);


            EncryptionKeyBitsPreference = new IntegerSetting(settings[Constants.EncryptionKeyBitsPreferenceSetting],
		        Constants.DefaultKeySizeToUseInActualEncryptionInBits);

			MaxPageSize =
				new IntegerSettingWithMin(settings["Raven/MaxPageSize"], 1024, 10);
			MemoryCacheLimitMegabytes =
				new IntegerSetting(settings["Raven/MemoryCacheLimitMegabytes"], GetDefaultMemoryCacheLimitMegabytes);
			MemoryCacheExpiration =
				new TimeSpanSetting(settings["Raven/MemoryCacheExpiration"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromSeconds);
			MemoryCacheLimitPercentage =
				new IntegerSetting(settings["Raven/MemoryCacheLimitPercentage"], 0 /* auto size */);
			MemoryCacheLimitCheckInterval =
				new TimeSpanSetting(settings["Raven/MemoryCacheLimitCheckInterval"], MemoryCache.Default.PollingInterval,
				                    TimeSpanArgumentType.FromParse);

            PrewarmFacetsSyncronousWaitTime =
                new TimeSpanSetting(settings["Raven/PrewarmFacetsSyncronousWaitTime"], TimeSpan.FromSeconds(3),
                                    TimeSpanArgumentType.FromParse);

            PrewarmFacetsOnIndexingMaxAge =
                new TimeSpanSetting(settings["Raven/PrewarmFacetsOnIndexingMaxAge"], TimeSpan.FromMinutes(10),
                                    TimeSpanArgumentType.FromParse);
			
			
			MaxIndexingRunLatency =
				new TimeSpanSetting(settings["Raven/MaxIndexingRunLatency"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromParse);
			MaxIndexWritesBeforeRecreate =
				new IntegerSetting(settings["Raven/MaxIndexWritesBeforeRecreate"], 256 * 1024);
			PreventAutomaticSuggestionCreation =
				new BooleanSetting(settings["Raven/PreventAutomaticSuggestionCreation"], false);
			DisablePerformanceCounters =
				new BooleanSetting(settings["Raven/DisablePerformanceCounters"], false);

			MaxNumberOfItemsToIndexInSingleBatch =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToIndexInSingleBatch"],
				                          defaultMaxNumberOfItemsToIndexInSingleBatch, 128);
			AvailableMemoryForRaisingIndexBatchSizeLimit =
				new IntegerSetting(settings["Raven/AvailableMemoryForRaisingIndexBatchSizeLimit"],
				                   Math.Min(768, MemoryStatistics.TotalPhysicalMemory/2));
			MaxNumberOfItemsToReduceInSingleBatch =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToReduceInSingleBatch"],
				                          defaultMaxNumberOfItemsToIndexInSingleBatch/2, 128);
			NumberOfItemsToExecuteReduceInSingleStep =
				new IntegerSetting(settings["Raven/NumberOfItemsToExecuteReduceInSingleStep"], 1024);
			MaxNumberOfParallelIndexTasks =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfParallelIndexTasks"], Environment.ProcessorCount, 1);

			NewIndexInMemoryMaxMb =
				new MultipliedIntegerSetting(new IntegerSettingWithMin(settings["Raven/NewIndexInMemoryMaxMB"], 64, 1), 1024*1024);
			RunInMemory =
				new BooleanSetting(settings["Raven/RunInMemory"], false);
			CreateAutoIndexesForAdHocQueriesIfNeeded =
				new BooleanSetting(settings["Raven/CreateAutoIndexesForAdHocQueriesIfNeeded"], true);
			ResetIndexOnUncleanShutdown =
				new BooleanSetting(settings["Raven/ResetIndexOnUncleanShutdown"], false);
			DisableInMemoryIndexing =
				new BooleanSetting(settings["Raven/DisableInMemoryIndexing"], false);
			DataDir =
				new StringSetting(settings["Raven/DataDir"], @"~\Data");
			IndexStoragePath =
				new StringSetting(settings["Raven/IndexStoragePath"], (string) null);
			HostName =
				new StringSetting(settings["Raven/HostName"], (string) null);
			Port =
				new StringSetting(settings["Raven/Port"], (string) null);
			UseSsl = 
				new BooleanSetting(settings["Raven/UseSsl"], false);
			HttpCompression =
				new BooleanSetting(settings["Raven/HttpCompression"], true);
			AccessControlAllowOrigin =
				new StringSetting(settings["Raven/AccessControlAllowOrigin"], (string) null);
			AccessControlMaxAge =
				new StringSetting(settings["Raven/AccessControlMaxAge"], "1728000" /* 20 days */);
			AccessControlAllowMethods =
				new StringSetting(settings["Raven/AccessControlAllowMethods"], "PUT,PATCH,GET,DELETE,POST");
			AccessControlRequestHeaders =
				new StringSetting(settings["Raven/AccessControlRequestHeaders"], (string) null);
			RedirectStudioUrl =
				new StringSetting(settings["Raven/RedirectStudioUrl"], (string) null);
			DisableDocumentPreFetchingForIndexing =
				new BooleanSetting(settings["Raven/DisableDocumentPreFetchingForIndexing"], false);
			MaxNumberOfItemsToPreFetchForIndexing =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToPreFetchForIndexing"],
										  defaultMaxNumberOfItemsToIndexInSingleBatch, 128);
			WebDir =
				new StringSetting(settings["Raven/WebDir"], GetDefaultWebDir);
			PluginsDirectory =
				new StringSetting(settings["Raven/PluginsDirectory"], @"~\Plugins");
			CompiledIndexCacheDirectory =
				new StringSetting(settings["Raven/CompiledIndexCacheDirectory"], @"~\Raven\CompiledIndexCache");
			TaskScheduler =
				new StringSetting(settings["Raven/TaskScheduler"], (string) null);
			AllowLocalAccessWithoutAuthorization =
				new BooleanSetting(settings["Raven/AllowLocalAccessWithoutAuthorization"], false);
			MaxIndexCommitPointStoreTimeInterval =
				new TimeSpanSetting(settings["Raven/MaxIndexCommitPointStoreTimeInterval"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromParse);
			MaxNumberOfStoredCommitPoints =
				new IntegerSetting(settings["Raven/MaxNumberOfStoredCommitPoints"], 5);
			MinIndexingTimeIntervalToStoreCommitPoint =
				new TimeSpanSetting(settings["Raven/MinIndexingTimeIntervalToStoreCommitPoint"], TimeSpan.FromMinutes(1),
				                    TimeSpanArgumentType.FromParse);
            
			TimeToWaitBeforeRunningIdleIndexes = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeRunningIdleIndexes"], TimeSpan.FromMinutes(10), TimeSpanArgumentType.FromParse);

			DatbaseOperationTimeout = new TimeSpanSetting(settings["Raven/DatbaseOperationTimeout"], TimeSpan.FromMinutes(5), TimeSpanArgumentType.FromParse);
            
			TimeToWaitBeforeMarkingAutoIndexAsIdle = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeMarkingAutoIndexAsIdle"], TimeSpan.FromHours(1), TimeSpanArgumentType.FromParse);

			TimeToWaitBeforeMarkingIdleIndexAsAbandoned = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeMarkingIdleIndexAsAbandoned"], TimeSpan.FromHours(72), TimeSpanArgumentType.FromParse);

			TimeToWaitBeforeRunningAbandonedIndexes = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeRunningAbandonedIndexes"], TimeSpan.FromHours(3), TimeSpanArgumentType.FromParse);

			DisableClusterDiscovery = new BooleanSetting(settings["Raven/DisableClusterDiscovery"], false);

			ClusterName = new StringSetting(settings["Raven/ClusterName"], (string)null);
			ServerName = new StringSetting(settings["Raven/ServerName"], (string)null);

			MaxStepsForScript = new IntegerSetting(settings["Raven/MaxStepsForScript"], 10*1000);
			AdditionalStepsForScriptBasedOnDocumentSize = new IntegerSetting(settings["Raven/AdditionalStepsForScriptBasedOnDocumentSize"], 5);

			FetchingDocumentsFromDiskTimeoutInSeconds = new IntegerSetting(settings["Raven/Prefetcher/FetchingDocumentsFromDiskTimeout"], 5);

			MaximumSizeAllowedToFetchFromStorageInMb = new IntegerSetting(settings["Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage"], 256);

			FlushIndexToDiskSizeInMb = new IntegerSetting(settings["Raven/Indexing/FlushIndexToDiskSizeInMb"], 5);

			MaxConcurrentDatabaseLoads = new IntegerSetting(settings[Constants.RavenMaxConcurrentDatabaseLoads], 8);
			ConcurrentDatabaseLoadTimeout = new TimeSpanSetting(settings[Constants.ConcurrentDatabaseLoadTimeout],
				TimeSpan.FromSeconds(15),
				TimeSpanArgumentType.FromParse);

			UseNullDocumentCacher = new BooleanSetting(settings["Raven/UseNullDocumentCacher"],false);
            SqlCommandTimeoutInSec = new IntegerSetting(settings["Raven/SqlReplication/CommandTimeoutInSec"], -1);
        }

	    private string GetDefaultWebDir()
		{
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
		}

		private int GetDefaultMemoryCacheLimitMegabytes()
		{
			var cacheSizeMaxSetting = new IntegerSetting(settings["Raven/Esent/CacheSizeMax"], 1024);

			// we need to leave ( a lot ) of room for other things as well, so we min the cache size
			var val = (MemoryStatistics.TotalPhysicalMemory/2) -
			          // reduce the unmanaged cache size from the default min
									cacheSizeMaxSetting.Value;

			if (val < 0)
				return 128; // if machine has less than 1024 MB, then only use 128 MB 

			return val;
		}

		public IntegerSetting MaxConcurrentDatabaseLoads { get; private set; }

		public TimeSpanSetting ConcurrentDatabaseLoadTimeout { get; private set; }

		public IntegerSetting MaxConcurrentServerRequests { get; private set; }

		public IntegerSetting MaxConcurrentRequestsForDatabaseDuringLoad { get; private set; }

        public IntegerSetting MaxSecondsForTaskToWaitForDatabaseToLoad { get; private set; }

		public IntegerSetting MaxConcurrentMultiGetRequests { get; private set; }

		public IntegerSetting MemoryLimitForIndexing { get; private set; }

        public IntegerSettingWithMin MinThreadPoolCompletionThreads { get; private set; }

        public IntegerSettingWithMin MinThreadPoolWorkerThreads { get; private set; }

        public IntegerSetting EncryptionKeyBitsPreference { get; private set; }

		public IntegerSettingWithMin MaxPageSize { get; private set; }

		public IntegerSetting MemoryCacheLimitMegabytes { get; private set; }

		public TimeSpanSetting MemoryCacheExpiration { get; private set; }

		public IntegerSetting MemoryCacheLimitPercentage { get; private set; }

		public TimeSpanSetting MemoryCacheLimitCheckInterval { get; private set; }

		public TimeSpanSetting MaxIndexingRunLatency { get; private set; }

        public TimeSpanSetting PrewarmFacetsOnIndexingMaxAge { get; private set; }

        public TimeSpanSetting PrewarmFacetsSyncronousWaitTime { get; private set; }

        public IntegerSettingWithMin MaxNumberOfItemsToIndexInSingleBatch { get; private set; }

		public IntegerSetting AvailableMemoryForRaisingIndexBatchSizeLimit { get; private set; }

		public IntegerSettingWithMin MaxNumberOfItemsToReduceInSingleBatch { get; private set; }

		public IntegerSetting NumberOfItemsToExecuteReduceInSingleStep { get; private set; }

		public IntegerSettingWithMin MaxNumberOfParallelIndexTasks { get; private set; }

		public MultipliedIntegerSetting NewIndexInMemoryMaxMb { get; private set; }

		public BooleanSetting RunInMemory { get; private set; }

		public BooleanSetting CreateAutoIndexesForAdHocQueriesIfNeeded { get; private set; }

		public BooleanSetting ResetIndexOnUncleanShutdown { get; private set; }

		public BooleanSetting DisableInMemoryIndexing { get; private set; }

		public StringSetting DataDir { get; private set; }

		public StringSetting IndexStoragePath { get; private set; }

		public StringSetting HostName { get; private set; }

		public StringSetting Port { get; private set; }

		public StringSetting SslCertificatePath { get; private set; }

		public StringSetting SslCertificatePassword { get; private set; }

		public BooleanSetting UseSsl { get; private set; }

		public BooleanSetting HttpCompression { get; private set; }

		public StringSetting AccessControlAllowOrigin { get; private set; }

		public StringSetting AccessControlMaxAge { get; private set; }

		public StringSetting AccessControlAllowMethods { get; private set; }

		public StringSetting AccessControlRequestHeaders { get; private set; }

		public StringSetting RedirectStudioUrl { get; private set; }

		public BooleanSetting DisableDocumentPreFetchingForIndexing { get; private set; }

		public IntegerSettingWithMin MaxNumberOfItemsToPreFetchForIndexing { get; private set; }

		public StringSetting WebDir { get; private set; }

		public BooleanSetting DisableClusterDiscovery { get; private set; }

		public StringSetting ClusterName { get; private set; }

		public StringSetting ServerName { get; private set; }

		public StringSetting PluginsDirectory { get; private set; }

		public StringSetting CompiledIndexCacheDirectory { get; private set; }

		public StringSetting TaskScheduler { get; private set; }

		public BooleanSetting AllowLocalAccessWithoutAuthorization { get; private set; }

		public TimeSpanSetting MaxIndexCommitPointStoreTimeInterval { get; private set; }

		public TimeSpanSetting MinIndexingTimeIntervalToStoreCommitPoint { get; private set; }

		public IntegerSetting MaxNumberOfStoredCommitPoints { get; private set; }
        public TimeSpanSetting TimeToWaitBeforeRunningIdleIndexes { get; private set; }

	    public TimeSpanSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; private set; }

		public TimeSpanSetting TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; private set; }

		public TimeSpanSetting TimeToWaitBeforeRunningAbandonedIndexes { get; private set; }
		
		public IntegerSetting MaxStepsForScript { get; private set; }

		public IntegerSetting AdditionalStepsForScriptBasedOnDocumentSize { get; private set; }

		public IntegerSetting MaxIndexWritesBeforeRecreate { get; private set; }

		public BooleanSetting PreventAutomaticSuggestionCreation { get; set; }
    
        public BooleanSetting DisablePerformanceCounters { get; set; }
		public TimeSpanSetting DatbaseOperationTimeout { get; private set; }

		public IntegerSetting MaximumSizeAllowedToFetchFromStorageInMb { get; set; }

		public IntegerSetting FlushIndexToDiskSizeInMb { get; set; }

		public IntegerSetting FetchingDocumentsFromDiskTimeoutInSeconds { get; set; }
		public BooleanSetting UseNullDocumentCacher { get; set; }
	    public IntegerSetting SqlCommandTimeoutInSec { get; set; }
	}
}