﻿using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using System.Threading.Tasks;
using TestingEnvironment.Client;

// ReSharper disable InconsistentNaming
// ReSharper disable FieldCanBeMadeReadOnly.Local

namespace TaskCleaner
{
    public class TaskCleaner : BaseTest
    {
        public TaskCleaner(string orchestratorUrl, string testName) : base(orchestratorUrl, testName, "Egor")
        {
        }

        public override void RunActualTest()
        {
            DoWork();
        }

        public void DoWork()
        {
            var dbs = DocumentStore.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue)).Result;
            RemoveBackupTasks(dbs).Wait();
        }

        private async Task RemoveBackupTasks(string[] dbs)
        {
            var success = true;
            var tasksCount = 0;
            foreach (var db in dbs)
            {
                var dbRecord = await DocumentStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(db));
                var pbTasksCount = dbRecord.PeriodicBackups.Count;
                var counter = 0;
                foreach (var pb in dbRecord.PeriodicBackups)
                {
                    await DocumentStore.Maintenance.SendAsync(new DeleteOngoingTaskOperation(pb.TaskId, OngoingTaskType.Backup));
                    var ongoingTask = await DocumentStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(pb.TaskId, OngoingTaskType.Backup));

                    if (ongoingTask == null)
                        counter++;
                    else
                    {
                        ReportFailure($"Failed to delete {nameof(OngoingTaskType.Backup)} task {pb.Name} with Id {pb.TaskId}", null);
                        success = false;
                    }
                }

                if (pbTasksCount == counter)
                {
                    ReportInfo($"Successfully removed all {pbTasksCount} {nameof(OngoingTaskType.Backup)} tasks, from {db} database.");
                }
                else
                {
                    ReportInfo($"Removed {counter} from {pbTasksCount} {nameof(OngoingTaskType.Backup)} tasks, from {db} database.");
                    success = false;
                }

                tasksCount += counter;
            }

            if(success)
                ReportSuccess($"Successfully removed all {tasksCount} {nameof(OngoingTaskType.Backup)} tasks, from all databases.");
            else
                ReportFailure($"Failed to removed all {nameof(OngoingTaskType.Backup)} tasks", null);
        }
    }
}