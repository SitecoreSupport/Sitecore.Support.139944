namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Runtime.Serialization;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.Data;
    using Sitecore.Data.Engines;
    using Sitecore.Services;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Maintenance;

    /// <summary>
    /// Defines the Index Rebuild Strategy on async update
    /// </summary>
    [DataContract]
    public class IntervalAsynchronousStrategy : BaseAsynchronousStrategy
    {
        /// <summary>
        /// The alarm clock.
        /// </summary>
        private AlarmClock alarmClock;

        /// <summary>
        /// The update interval.
        /// </summary>
        private TimeSpan updateInterval;

        /// <summary>
        /// Initializes a new instance of the <see cref="IntervalAsynchronousStrategy"/> class.
        /// </summary>
        /// <param name="database">
        /// The database.
        /// </param>
        /// <param name="interval">
        /// The interval.
        /// </param>
        public IntervalAsynchronousStrategy(string database, string interval = null) : base(database)
        {
            this.updateInterval = DateUtil.ParseTimeSpan(interval, Configuration.Settings.Indexing.UpdateInterval, CultureInfo.CurrentCulture);
            this.alarmClock = new AlarmClock(this.updateInterval);
        }

        /// <summary>
        /// The initialize.
        /// </summary>
        /// <param name="searchIndex">
        /// The index.
        /// </param>
        public override void Initialize(ISearchIndex searchIndex)
        {
            base.Initialize(searchIndex);

            if (this.Settings.EnableEventQueues())
            {
                this.alarmClock.Ring += (sender, args) => this.Handle();
            }
        }

        /// <summary>
        /// The run.
        /// </summary>
        public override void Run()
        {
            if (IndexCustodian.IsIndexingPaused(this.Index))
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}] IntervalAsynchronousUpdateStrategy triggered but muted. Indexing is paused.", this.Index.Name));
                return;
            }

            if (IndexCustodian.IsRebuilding(this.Index))
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}] IntervalAsynchronousUpdateStrategy triggered but muted. Index is being built at the moment.", this.Index.Name));
                return;
            }

            base.Run();
        }

        /// <summary>The handle.</summary>
        /// <param name="args">The args.</param>
        [Obsolete("Handle(EventArgs) method is no longer in use and will be removed in later release.")]
        protected void Handle(EventArgs args)
        {
            OperationMonitor.Register(this.Run);
            OperationMonitor.Trigger();
        }

        /// <summary>
        /// Extracts uris and timestamps from collectedHistory.
        /// </summary>
        /// <param name="queue">The event collectedHistory.</param>
        /// <returns><see cref="Dictionary{TKey,TValue}"/></returns>
        [Obsolete("ExtractUriAndTimestamp(List<HistoryEntry>) method is no longer in use and will be removed in later release.")]
        protected Dictionary<DataUri, DateTime> ExtractUriAndTimestamp(List<HistoryEntry> queue)
        {
            var data = new Dictionary<DataUri, DateTime>();

            foreach (var entry in queue)
            {
                // does not work in Sitecore < 8.0
                var uri = new DataUri(ID.Parse(entry.ItemId), entry.ItemLanguage, entry.ItemVersion);
                data[uri] = entry.Created;
            }

            return data;
        }

        /// <summary>
        /// Extracts instances of <see cref="IndexableInfo" /> from collectedHistory.
        /// </summary>
        /// <param name="collectedHistory">The collected history.</param>
        /// <returns><see cref="IEnumerable{T}" /> </returns>
        [Obsolete("ExtractIndexableInfo(List<HistoryEntry>) method is no longer in use and will be removed in later release.")]
        protected IEnumerable<IndexableInfo> ExtractIndexableInfo(List<HistoryEntry> collectedHistory)
        {
            return this.ExtractUriAndTimestamp(collectedHistory)
                .Select(de => new IndexableInfo(new SitecoreItemUniqueId(new ItemUri(de.Key.ItemID, de.Key.Language, de.Key.Version, this.Database)), de.Value))
                .GroupBy(x => x.IndexableUniqueId)
                .Select(group => group.FirstOrDefault(x => x.Created == group.Max(i => i.Created)))
                .OrderBy(x => x.Created).ToList();
        }
    }
}