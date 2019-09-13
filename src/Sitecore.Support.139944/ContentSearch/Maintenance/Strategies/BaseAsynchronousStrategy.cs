using System.Threading;
using Sitecore.Data.Archiving;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.Maintenance.Strategies;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.Data;
    using Sitecore.Data.Eventing.Remote;
    using Sitecore.Diagnostics;
    using Sitecore.Eventing;
    using Sitecore.Globalization;
    using Sitecore.Jobs;

    /// <summary>
    /// Defines the Index Rebuild Strategy on async update
    /// </summary>
    public abstract partial class BaseAsynchronousStrategy : IIndexUpdateStrategy
    {
        /// <summary>
        /// Determines if strategy initialized
        /// </summary>
        private volatile int initialized;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseAsynchronousStrategy"/> class.
        /// </summary>
        /// <param name="database">The database.</param>
        protected BaseAsynchronousStrategy(string database)
        {
            Assert.IsNotNullOrEmpty(database, "database");
            this.Database = ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(database);
            Assert.IsNotNull(this.Database, string.Format("Database '{0}' was not found", database));
        }

        /// <summary>
        /// Gets or sets the database.
        /// </summary>
        public Database Database { get; protected set; }

        /// <summary>
        /// Gets or sets a value indicating whether check for threshold.
        /// </summary>
        public bool CheckForThreshold { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether strategy should raise remote events.
        /// </summary>
        public bool RaiseRemoteEvents { get; set; }

        /// <summary>
        /// Gets or sets the settings.
        /// </summary>
        internal Sitecore.ContentSearch.Abstractions.ISettings Settings { get; set; }

        /// <summary>
        /// Gets or sets the index.
        /// </summary>
        protected ISearchIndex Index { get; set; }

        /// <summary>
        /// Gets or sets the content search configuration settings.
        /// </summary>
        protected IContentSearchConfigurationSettings ContentSearchSettings { get; set; }

        /// <summary>
        /// Gets or sets the collection of index timestamps.
        /// </summary>
        protected Dictionary<string, long> IndexTimestamps { get; set; }

        /// <summary>The initialize.</summary>
        /// <param name="searchIndex">The index.</param>
        public virtual void Initialize(ISearchIndex searchIndex)
        {
            Assert.IsNotNull(searchIndex, "searchIndex");
            CrawlingLog.Log.Info(string.Format("[Index={0}] Initializing {1}.", searchIndex.Name, this.GetType().Name));

            if (Interlocked.CompareExchange(ref this.initialized, 1, 0) == 0)
            {
                this.Index = searchIndex;
                this.Settings = this.Index.Locator.GetInstance<Sitecore.ContentSearch.Abstractions.ISettings>();
                this.ContentSearchSettings = this.Index.Locator.GetInstance<IContentSearchConfigurationSettings>();

                if (!this.Settings.EnableEventQueues())
                {
                    CrawlingLog.Log.Fatal(string.Format("[Index={0}] Initialization of {1} failed because event queue is not enabled.", searchIndex.Name, this.GetType().Name));
                }
                else
                {
                    if (this.IndexTimestamps == null)
                    {
                        this.IndexTimestamps = new Dictionary<string, long>();
                    }

                    EventHub.OnIndexingStarted += this.OnIndexingStarted;
                    EventHub.OnIndexingEnded += this.OnIndexingEnded;
                }
            }
        }

        /// <summary>
        /// Runs the pipeline.
        /// </summary>
        public virtual void Run()
        {
            EventManager.RaiseQueuedEvents(); // in order to prevent indexing out-of-date data we have to force processing queued events before reading queue.

            var eventQueue = this.Database.RemoteEvents.Queue;

            if (eventQueue == null)
            {
                CrawlingLog.Log.Fatal(string.Format("Event Queue is empty. Returning."));
                return;
            }

            var queue = this.ReadQueue(eventQueue);

            this.Run(queue, this.Index);
        }

        #region Protected methods

        /// <summary>
        /// The handle.
        /// </summary>
        protected void Handle()
        {
            OperationMonitor.Register(this.Run, this.Index.Name);
            OperationMonitor.Trigger();
        }

        /// <summary>
        /// The read queue.
        /// </summary>
        /// <param name="eventQueue">The event queue.</param>
        /// <returns>
        /// The <see cref="List{QueuedEvent}" />.
        /// </returns>
        protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue)
        {
            return this.ReadQueue(eventQueue, this.Index.Summary.LastUpdatedTimestamp);
        }

        /// <summary>
        /// Reads the queue.
        /// </summary>
        /// <param name="eventQueue">The event queue.</param>
        /// <param name="lastUpdatedTimestamp">The last updated timestamp.</param>
        /// <returns>
        /// The list of queued events.
        /// </returns>
        protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue, long? lastUpdatedTimestamp)
        {
            var queue = new List<QueuedEvent>();

            lastUpdatedTimestamp = lastUpdatedTimestamp ?? 0;

            var query = new EventQueueQuery { FromTimestamp = lastUpdatedTimestamp };
            query.EventTypes.Add(typeof(RemovedVersionRemoteEvent));
            query.EventTypes.Add(typeof(SavedItemRemoteEvent));
            query.EventTypes.Add(typeof(DeletedItemRemoteEvent));
            query.EventTypes.Add(typeof(MovedItemRemoteEvent));
            query.EventTypes.Add(typeof(AddedVersionRemoteEvent));
            query.EventTypes.Add(typeof(CopiedItemRemoteEvent));
            query.EventTypes.Add(typeof(RestoreItemCompletedEvent));

            queue.AddRange(eventQueue.GetQueuedEvents(query));

            return queue.Where(e => e.Timestamp > lastUpdatedTimestamp).ToList();
        }

        /// <summary>
        /// Extracts instances of <see cref="IndexableInfo"/> from queue.
        /// </summary>
        /// <param name="queue">The event queue.</param>
        /// <returns><see cref="IEnumerable{T}"/></returns>
        protected IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
        {
            var indexableListToUpdate = new DataUriBucketDictionary<IndexableInfo>();
            var indexableListToRemove = new DataUriBucketDictionary<IndexableInfo>();

            foreach (var queuedEvent in queue)
            {
                var instanceData = this.Database.RemoteEvents.Queue.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

                if (instanceData == null)
                {
                    continue;
                }

                var key = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber));
                var itemUri = new ItemUri(key.ItemID, key.Language, key.Version, this.Database);
                var indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), queuedEvent.Timestamp);

                if (instanceData is RemovedVersionRemoteEvent || instanceData is DeletedItemRemoteEvent)
                {
                    this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
                }
                else if (instanceData is AddedVersionRemoteEvent)
                {
                    this.HandleIndexableToAddVersion(indexableListToUpdate, key, indexable);
                }
                else
                {
                    this.UpdateIndexableInfo(instanceData, indexable);
                    this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
                }
            }

            return indexableListToUpdate.ExtractValues()
              .Concat(indexableListToRemove.ExtractValues())
              .OrderBy(x => x.Timestamp).ToList();
        }

        /// <summary>
        /// Runs the specified queue.
        /// </summary>
        /// <param name="queue">The queue.</param>
        /// <param name="index">The index.</param>
        protected virtual void Run(List<QueuedEvent> queue, ISearchIndex index)
        {
            CrawlingLog.Log.Debug(string.Format("[Index={0}] {1} executing.", index.Name, this.GetType().Name));

            if (this.Database == null)
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", index.Name));
                return;
            }

            var lastUpdated = index.Summary.LastUpdatedTimestamp ?? 0;
            queue = queue.Where(q => q.Timestamp > lastUpdated).ToList();

            if (queue.Count <= 0)
            {
                CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", index.Name));
                return;
            }

            if (this.CheckForThreshold && queue.Count > this.ContentSearchSettings.FullRebuildItemCountThreshold())
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", index.Name, this.ContentSearchSettings.FullRebuildItemCountThreshold()));
                if (this.RaiseRemoteEvents)
                {
                    Job fullRebuildJob = IndexCustodian.FullRebuild(index);
                    fullRebuildJob.Wait();
                }
                else
                {
                    Job fullRebuildRemoteJob = IndexCustodian.FullRebuildRemote(index);
                    fullRebuildRemoteJob.Wait();
                }

                return;
            }

            List<IndexableInfo> parsed = this.ExtractIndexableInfoFromQueue(queue).ToList();
            CrawlingLog.Log.Info(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", index.Name, parsed.Count()));

            Job incrememtalUpdateJob = IndexCustodian.IncrementalUpdate(index, parsed);
            incrememtalUpdateJob.Wait();
        }

        /// <summary>
        /// The indexing started handler.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="args">The args.</param>
        protected virtual void OnIndexingStarted(object sender, EventArgs args)
        {
            var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
            var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

            if (this.Index.Name == indexName && isFullRebuild)
            {
                QueuedEvent lastEvent = this.Database.RemoteEvents.Queue.GetLastEvent();
                this.IndexTimestamps[indexName] = lastEvent == null ? 0 : lastEvent.Timestamp;
            }
        }

        /// <summary>
        /// The indexing ended handler.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="args">The args.</param>
        protected virtual void OnIndexingEnded(object sender, EventArgs args)
        {
            var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
            var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

            if (this.Index.Name == indexName && isFullRebuild && this.IndexTimestamps.ContainsKey(indexName))
            {
                this.Index.Summary.LastUpdatedTimestamp = this.IndexTimestamps[indexName];
            }
        }

        #endregion

        #region Private methods

        /// <summary>
        /// Updates the indexable info.
        /// </summary>
        /// <param name="instanceData">The instance data.</param>
        /// <param name="indexable">The indexable.</param>
        private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
        {
            if (instanceData is SavedItemRemoteEvent)
            {
                var savedEvent = instanceData as SavedItemRemoteEvent;

                if (savedEvent.IsSharedFieldChanged)
                {
                    indexable.IsSharedFieldChanged = true;
                }

                if (savedEvent.IsUnversionedFieldChanged)
                {
                    indexable.IsUnversionedFieldChanged = true;
                }
            }

            if (instanceData is RestoreItemCompletedEvent)
            {
                indexable.IsSharedFieldChanged = true;
            }

            if (instanceData is CopiedItemRemoteEvent)
            {
                indexable.IsSharedFieldChanged = true;
                var copiedItemData = instanceData as CopiedItemRemoteEvent;
                if (copiedItemData.Deep)
                {
                    indexable.NeedUpdateChildren = true;
                }
            }

            var @event = instanceData as MovedItemRemoteEvent;
            if (@event != null)
            {
                indexable.NeedUpdateChildren = true;
                indexable.OldParentId = @event.OldParentId;
            }
        }

        /// <summary>
        /// Processes indexable that contains data about item or version removed events.
        /// </summary>
        /// <param name="collection">The indexable collection.</param>
        /// <param name="key">The indexable key.</param>
        /// <param name="indexable">The indexable data.</param>
        private void HandleIndexableToRemove(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            if (collection.ContainsKey(key))
            {
                collection[key].Timestamp = indexable.Timestamp;
            }
            else
            {
                collection.Add(key, indexable);
            }
        }

        /// <summary>
        /// Handles the indexable to add version.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="key">The key.</param>
        /// <param name="indexable">The indexable.</param>
        private void HandleIndexableToAddVersion(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            indexable.IsVersionAdded = true;
            if (!collection.ContainsKey(key))
            {
                collection.Add(key, indexable);
            }
            else
            {
                collection[key].Timestamp = indexable.Timestamp;
                collection[key].IsVersionAdded = true;
            }
        }

        /// <summary>
        /// Processes indexable that contains data about item or version removed events.
        /// </summary>
        /// <param name="collection">The indexable collection.</param>
        /// <param name="key">The indexable key.</param>
        /// <param name="indexable">The indexable data.</param>
        private void HandleIndexableToUpdate(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            bool alreadySetNeedUpdateChildren = collection.ContainsAny(key.ItemID, x => x.Value.NeedUpdateChildren);
            bool alreadyAddedSharedFieldChange = collection.ContainsAny(key.ItemID, x => x.Value.IsSharedFieldChanged);
            bool alreadyAddedUnversionedFieldChange = collection.ContainsAny(key.ItemID, x => x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);

            if (alreadySetNeedUpdateChildren || alreadyAddedSharedFieldChange)
            {
                var entry = collection.First(key.ItemID);
                entry.Timestamp = indexable.Timestamp;
                entry.NeedUpdateChildren = entry.NeedUpdateChildren || indexable.NeedUpdateChildren;
            }
            else if (indexable.IsSharedFieldChanged || indexable.NeedUpdateChildren)
            {
                collection.RemoveAll(key.ItemID);
                collection.Add(key, indexable);
            }
            else if (alreadyAddedUnversionedFieldChange)
            {
                collection.First(key.ItemID, x => x.Key.Language == key.Language).Timestamp = indexable.Timestamp;
            }
            else if (indexable.IsUnversionedFieldChanged)
            {
                collection.RemoveAll(key.ItemID);
                collection.Add(key, indexable);
            }
            else
            {
                if (collection.ContainsKey(key))
                {
                    collection[key].Timestamp = indexable.Timestamp;
                }
                else
                {
                    collection.Add(key, indexable);
                }
            }
        }

        #endregion
    }
}