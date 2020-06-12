using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using PortalRecordsMover.AppCode;

namespace PortalRecordsMover.AppCode
{
    internal class RecordManager
    {
        private const int maxErrorLoopCount = 5;
        private readonly List<EntityReference> recordsToDeactivate;
        private readonly IOrganizationService service;

        public RecordManager(IOrganizationService service)
        {
            this.service = service;
            recordsToDeactivate = new List<EntityReference>();
        }

        public bool ProcessRecords(EntityCollection ec, List<EntityMetadata> emds)
        {
            var records = new List<Entity>(ec.Entities);
            var progress = new ImportProgress(records.Count);

            var nextCycle = new List<Entity>();
            int loopIndex = 0;
            while (records.Any())
            {
                loopIndex++;
                if (loopIndex == maxErrorLoopCount)
                {
                    break;
                }

                for (int i = records.Count - 1; i >= 0; i--)
                {
                    EntityProgress entityProgress;

                    var record = records[i];
                    // Handle annotations.  
                    // TODO review this section 
                    if (record.LogicalName != "annotation")
                    {
                        // see if any records in the entity collection have a reference to this Annotation 
                        if (record.Attributes.Values.Any(v => v is EntityReference && records.Select(r => r.Id).Contains(((EntityReference)v).Id)))
                        {
                            if (nextCycle.Any(r => r.Id == record.Id)) {
                                continue;
                            }

                            var newRecord = new Entity(record.LogicalName) { Id = record.Id };
                            var toRemove = new List<string>();
                            foreach (var attr in record.Attributes) 
                            {
                                if (attr.Value is EntityReference) 
                                {
                                    newRecord.Attributes.Add(attr.Key, attr.Value);
                                    toRemove.Add(attr.Key);
                                    nextCycle.Add(newRecord);
                                }
                            }

                            foreach (var attr in toRemove) {
                                record.Attributes.Remove(attr);
                            }
                        }
                        // 
                        if (record.Attributes.Values.Any(v => (v is Guid) && records.Where(r => r.Id != record.Id).Select(r => r.Id).Contains((Guid)v))) {
                            continue;
                        }
                    }

                    // update the entity progress element 
                    TrackEntityProgress();

                    var name = string.IsNullOrEmpty(entityProgress.Metadata.PrimaryNameAttribute)
                        ? "(N/A)"
                        : record.GetAttributeValue<string>(entityProgress.Metadata.PrimaryNameAttribute);

                    try
                    {
                        record.Attributes.Remove("ownerid");

                        if (record.Attributes.Contains("statecode") &&
                            record.GetAttributeValue<OptionSetValue>("statecode").Value == 1)
                        {
                            PortalMover.ReportProgress($"Record {name} ({record.Id}) is inactive : Added for deactivation step");

                            recordsToDeactivate.Add(record.ToEntityReference());
                            record.Attributes.Remove("statecode");
                            record.Attributes.Remove("statuscode");
                        }

                        // check to see if this is an N:N relation vs a standard record import.
                        if (record.Attributes.Count == 3 && record.Attributes.Values.All(v => v is Guid))
                        {
                            try
                            {
                                // perform the association!
                                var rel = emds.SelectMany(e => e.ManyToManyRelationships).First(r => r.IntersectEntityName == record.LogicalName);
                                
                                service.Associate(
                                    rel.Entity1LogicalName,
                                    record.GetAttributeValue<Guid>(rel.Entity1IntersectAttribute),
                                    new Relationship(rel.SchemaName),
                                    new EntityReferenceCollection(new List<EntityReference>
                                    {
                                        new EntityReference(rel.Entity2LogicalName, record.GetAttributeValue<Guid>(rel.Entity2IntersectAttribute))
                                    })
                                );

                                PortalMover.ReportProgress($"Import: Association {entityProgress.Entity} ({record.Id}) created");
                            }
                            catch (FaultException<OrganizationServiceFault> error)
                            {
                                if (error.Detail.ErrorCode != -2147220937) {
                                    throw;
                                }
                                PortalMover.ReportProgress($"Import: Association {entityProgress.Entity} ({record.Id}) already exists");
                            }
                        }
                        else
                        {
                            // Do the Insert/Update!
                            var result = (UpsertResponse)service.Execute(new UpsertRequest {
                                Target = record
                            });
                            PortalMover.ReportProgress($"Import: Record {record.GetAttributeValue<string>(entityProgress.Metadata.PrimaryNameAttribute)} {(result.RecordCreated ? "created" : "updated")} ({entityProgress.Entity}/{record.Id})");
                        }

                        records.RemoveAt(i);
                        entityProgress.Success++;
                        entityProgress.Processed++;
                    }
                    catch (Exception error)
                    {
                        PortalMover.ReportProgress($"Import: An error occured attempting the insert/update/associate: {record.GetAttributeValue<string>(entityProgress.Metadata.PrimaryNameAttribute)} ({entityProgress.Entity}/{record.Id}): {error.Message}");
                        entityProgress.Error++;
                    }

                    // track the progress of the current list of records being imported 
                    void TrackEntityProgress()
                    {
                        // get the entity progress for this record entity type.
                        entityProgress = progress.Entities.FirstOrDefault(e => e.LogicalName == record.LogicalName);
                        if (entityProgress == null) 
                        {
                            var emd = emds.First(e => e.LogicalName == record.LogicalName);
                            string displayName = emd.DisplayName?.UserLocalizedLabel?.Label;

                            if (displayName == null && emd.IsIntersect.Value) {
                                var rel = emds.SelectMany(ent => ent.ManyToManyRelationships)
                                .First(r => r.IntersectEntityName == emd.LogicalName);

                                displayName = $"{emds.First(ent => ent.LogicalName == rel.Entity1LogicalName).DisplayName?.UserLocalizedLabel?.Label} / {emds.First(ent => ent.LogicalName == rel.Entity2LogicalName).DisplayName?.UserLocalizedLabel?.Label}";
                            }
                            if (displayName == null) {
                                displayName = emd.SchemaName;
                            }

                            entityProgress = new EntityProgress(emd, displayName);
                            progress.Entities.Add(entityProgress);
                        }
                    }
                }
            }

            PortalMover.ReportProgress("Import: Updating records to add references");

            var count = nextCycle.DistinctBy(r => r.Id).Count();
            var index = 0;

            foreach (var record in nextCycle.DistinctBy(r => r.Id))
            {
                try
                {
                    index++;

                    PortalMover.ReportProgress($"Import: Upating record {record.LogicalName} ({record.Id})");

                    record.Attributes.Remove("ownerid");
                    service.Update(record);
                }
                catch (Exception error)
                {
                    PortalMover.ReportProgress ($"Import: An error occured during import: {error.Message}");
                }
            }
            return false;
        }

        public EntityCollection RetrieveRecords(EntityMetadata emd, Settings settings)
        {
            var query = new QueryExpression(emd.LogicalName)
            {
                ColumnSet = new ColumnSet(true),
                Criteria = new FilterExpression { Filters = { new FilterExpression(LogicalOperator.Or) } }
            };

            if (settings.Config.CreateFilter.HasValue)
            {
                query.Criteria.Filters[0].AddCondition("createdon", ConditionOperator.OnOrAfter, settings.Config.CreateFilter.Value.ToString("yyyy-MM-dd"));
            }

            if (settings.Config.ModifyFilter.HasValue)
            {
                query.Criteria.Filters[0].AddCondition("modifiedon", ConditionOperator.OnOrAfter, settings.Config.ModifyFilter.Value.ToString("yyyy-MM-dd"));
            }

            if (settings.Config.WebsiteFilter != Guid.Empty)
            {
                var lamd = emd.Attributes.FirstOrDefault(a =>
                    a is LookupAttributeMetadata metadata && metadata.Targets[0] == "adx_website");
                if (lamd != null)
                {
                    query.Criteria.AddCondition(lamd.LogicalName, ConditionOperator.Equal, settings.Config.WebsiteFilter);
                }
                else
                {
                    switch (emd.LogicalName)
                    {
                        case "adx_webfile":
                            var noteLe = new LinkEntity
                            {
                                LinkFromEntityName = "adx_webfile",
                                LinkFromAttributeName = "adx_webfileid",
                                LinkToAttributeName = "objectid",
                                LinkToEntityName = "annotation",
                                LinkCriteria = new FilterExpression(LogicalOperator.Or)
                            };

                            bool addLinkEntity = false;

                            if (settings.Config.CreateFilter.HasValue)
                            {
                                noteLe.LinkCriteria.AddCondition("createdon", ConditionOperator.OnOrAfter, settings.Config.CreateFilter.Value.ToString("yyyy-MM-dd"));
                                addLinkEntity = true;
                            }

                            if (settings.Config.ModifyFilter.HasValue)
                            {
                                noteLe.LinkCriteria.AddCondition("modifiedon", ConditionOperator.OnOrAfter, settings.Config.ModifyFilter.Value.ToString("yyyy-MM-dd"));
                                addLinkEntity = true;
                            }

                            if (addLinkEntity)
                            {
                                query.LinkEntities.Add(noteLe);
                            }
                            break;

                        case "adx_entityformmetadata":
                            query.LinkEntities.Add(
                                CreateParentEntityLinkToWebsite(
                                    emd.LogicalName,
                                    "adx_entityform",
                                    "adx_entityformid",
                                    "adx_entityform",
                                    settings.Config.WebsiteFilter));
                            break;

                        case "adx_webformmetadata":
                            var le = CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_webformstep",
                                "adx_webformstepid",
                                "adx_webformstep",
                                Guid.Empty);

                            le.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                "adx_webformstep",
                                "adx_webform",
                                "adx_webformid",
                                "adx_webform",
                                settings.Config.WebsiteFilter));

                            query.LinkEntities.Add(le);
                            break;

                        case "adx_weblink":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_weblinksetid",
                                "adx_weblinksetid",
                                "adx_weblinkset",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_blogpost":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_blogid",
                                "adx_blogid",
                                "adx_blog",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_communityforumaccesspermission":
                        case "adx_communityforumannouncement":
                        case "adx_communityforumthread":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_forumid",
                                "adx_communityforumid",
                                "adx_communityforum",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_communityforumpost":
                            var lef = CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_forumthreadid",
                                "adx_communityforumthreadid",
                                "adx_communityforumthread",
                                Guid.Empty);

                            lef.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                "adx_communityforumthread",
                                "adx_forumid",
                                "adx_communityforumid",
                                "adx_communityforum",
                                settings.Config.WebsiteFilter));

                            query.LinkEntities.Add(lef);

                            break;

                        case "adx_idea":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_ideaforumid",
                                "adx_ideaforumid",
                                "adx_ideaforum",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_pagealert":
                        case "adx_webpagehistory":
                        case "adx_webpagelog":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_webpageid",
                                "adx_webpageid",
                                "adx_webpage",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_pollsubmission":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_pollid",
                                "adx_pollid",
                                "adx_poll",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_webfilelog":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_webfileid",
                                "adx_webfileid",
                                "adx_webfile",
                                settings.Config.WebsiteFilter));
                            break;

                        case "adx_webformsession":
                        case "adx_webformstep":
                            query.LinkEntities.Add(CreateParentEntityLinkToWebsite(
                                emd.LogicalName,
                                "adx_webform",
                                "adx_webformid",
                                "adx_webform",
                                settings.Config.WebsiteFilter));
                            break;
                    }
                }
            }

            if (settings.Config.ActiveItemsOnly && emd.LogicalName != "annotation")
            {
                query.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            }

            return service.RetrieveMultiple(query);
        }

        private LinkEntity CreateParentEntityLinkToWebsite(string fromEntity, string fromAttribute, string toAttribute, string toEntity, Guid websiteId)
        {
            var le = new LinkEntity
            {
                LinkFromEntityName = fromEntity,
                LinkFromAttributeName = fromAttribute,
                LinkToAttributeName = toAttribute,
                LinkToEntityName = toEntity,
            };

            if (websiteId != Guid.Empty)
            {
                le.LinkCriteria.AddCondition("adx_websiteid", ConditionOperator.Equal, websiteId);
            }

            return le;
        }

    }
}