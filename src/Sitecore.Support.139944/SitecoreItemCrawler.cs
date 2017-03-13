using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.SecurityModel;
using System;
using System.Linq;
using System.Threading;
using Sitecore.ContentSearch;

namespace Sitecore.Support.ContentSearch
{
    public class SitecoreItemCrawler : Sitecore.ContentSearch.SitecoreItemCrawler
    {

        public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            this.Update(context, indexableUniqueId, null, indexingOptions);
        }

        public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexEntryOperationContext operationContext, IndexingOptions indexingOptions = IndexingOptions.Default)
        {
            Assert.ArgumentNotNull(indexableUniqueId, "indexableUniqueId");
            ITrackingIndexingContext trackingIndexingContext = context as ITrackingIndexingContext;
            if (trackingIndexingContext != null)
            {
                trackingIndexingContext.Processed.TryAdd(indexableUniqueId, null);
            }
            else
            {
                return;
            }
            if (!base.ShouldStartIndexing(indexingOptions))
            {
                return;
            }
            IDocumentBuilderOptions documentOptions = base.DocumentOptions;
            Assert.IsNotNull(documentOptions, "DocumentOptions");
            if (this.IsExcludedFromIndex(indexableUniqueId, operationContext, true))
            {
                return;
            }
            if (operationContext != null)
            {
                if (operationContext.NeedUpdateChildren)
                {
                    Item item = Sitecore.Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);
                    if (item != null)
                    {
                        bool flag = operationContext.OldParentId != Guid.Empty && this.IsRootOrDescendant(new ID(operationContext.OldParentId)) && !this.IsAncestorOf(item);
                        if (flag)
                        {
                            this.Delete(context, indexableUniqueId, IndexingOptions.Default);
                            return;
                        }
                        this.UpdateHierarchicalRecursive(context, item, CancellationToken.None);
                        return;
                    }
                }
                if (operationContext.NeedUpdatePreviousVersion)
                {
                    Item item2 = Sitecore.Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);
                    if (item2 != null)
                    {
                        this.UpdatePreviousVersion(item2, context);
                    }
                }
            }
            SitecoreIndexableItem indexableAndCheckDeletes = this.GetIndexableAndCheckDeletes(indexableUniqueId);
            if (indexableAndCheckDeletes != null)
            {
                this.DoUpdate(context, indexableAndCheckDeletes, operationContext);
                return;
            }
            if (this.GroupShouldBeDeleted(indexableUniqueId.GroupId))
            {
                this.Delete(context, indexableUniqueId.GroupId, IndexingOptions.Default);
                return;
            }
            this.Delete(context, indexableUniqueId, IndexingOptions.Default);
        }

        private bool IsRootOrDescendant(ID id)
        {
            if (this.RootItem.ID == id)
            {
                return true;
            }
            Abstractions.IFactory instance = ContentSearchManager.Locator.GetInstance<Abstractions.IFactory>();
            Database database = instance.GetDatabase(this.Database);
            Item item;
            using (new SecurityDisabler())
            {
                item = database.GetItem(id);
            }
            return item != null && this.IsAncestorOf(item);
        }

        private void UpdatePreviousVersion(Item item, IProviderUpdateContext context)
        {
            Sitecore.Data.Version[] array;
            using (new WriteCachesDisabler())
            {
                array = (item.Versions.GetVersionNumbers() ?? new Sitecore.Data.Version[0]);
            }
            int num = array.ToList<Sitecore.Data.Version>().FindIndex((Sitecore.Data.Version version) => version.Number == item.Version.Number);
            if (num < 1)
            {
                return;
            }
            Sitecore.Data.Version previousVersion = array[num - 1];
            Sitecore.Data.Version version2 = array.FirstOrDefault((Sitecore.Data.Version version) => version == previousVersion);
            ItemUri uri = new ItemUri(item.ID, item.Language, version2, item.Database.Name);
            Item item2 = Sitecore.Data.Database.GetItem(uri);
            SitecoreIndexableItem sitecoreIndexableItem = item2;
            if (sitecoreIndexableItem != null)
            {
                IIndexableBuiltinFields indexableBuiltinFields = sitecoreIndexableItem;
                indexableBuiltinFields.IsLatestVersion = false;
                sitecoreIndexableItem.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;
                base.Operations.Update(sitecoreIndexableItem, context, this.index.Configuration);
            }
        }
    }
}