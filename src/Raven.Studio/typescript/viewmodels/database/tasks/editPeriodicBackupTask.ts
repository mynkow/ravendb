﻿import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePeriodicBackupConfigurationCommand = require("commands/database/tasks/savePeriodicBackupConfigurationCommand");
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import getPeriodicBackupConfigurationCommand = require("commands/database/tasks/getPeriodicBackupConfigurationCommand");
import testPeriodicBackupCredentialsCommand = require("commands/database/tasks/testPeriodicBackupCredentialsCommand");
import popoverUtils = require("common/popoverUtils");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class editPeriodicBackupTask extends viewModelBase {

    configuration = ko.observable<periodicBackupConfiguration>();

    constructor() {
        super();
        
        this.bindToCurrentInstance("testCredentials");
    }

    activate(args: any) { 
        super.activate(args);
        
        const deferred = $.Deferred<void>();
        
        if (args.taskId) {
            // editing an existing task
            new getPeriodicBackupConfigurationCommand(this.activeDatabase(), args.taskId)
                .execute()
                .done((configuration: Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration) => {
                    this.configuration(new periodicBackupConfiguration(configuration));
                    deferred.resolve();
                })
                .fail(() => {
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                    deferred.reject();
                });
        }
        else {
            // creating a new task
            this.configuration(periodicBackupConfiguration.empty());
            deferred.resolve();
        }

        return deferred;
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById("taskName").focus();
    }

    attached() {
        super.attached();

        $("#backup-info").popover({
            html: true,
            trigger: "hover",
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            content:
                "Differences between Backup and Snapshot:" +
                "<ul>" +
                    "<li>Data" +
                        "<ul>" +
                            "<li>Backup includes documents, indexes, transformers and identities <br> " +
                                "but doesn't include index data, indexes will be rebuilt after restore based on exported definitions</li>" +
                            "<li>Snapshot contains the raw data including the indexes - definitions and data</li>" +
                        "</ul>" +
                    "</li>" +
                    "<li>Speed" +
                        "<ul>" +
                            "<li>Backup is usually much faster than a Snapshot</li>" +
                        "</ul>" +
                    "</li>" +
                    "<li>Size" +
                        "<ul>" +
                            "<li>Backup is much smaller than Snapshot</li>" +
                        "</ul>" +
                    "</li>" +
                    "<li>Restore" +
                        "<ul>" +
                            "<li>Restore of a Snapshot is faster than of a Backup</li>" +
                        "</ul>" +
                    "</li>" +
                "</ul>" +
                "* An incremental Snapshot is the same as an incremental Backup"
        });

        $("#schedule-info").popover({
            html: true,
            trigger: "hover",
            template: popoverUtils.longPopoverTemplate,
            container: "body",
            content:
                "<div class='schedule-info-text'>" +  
                "Backup schedule is defined by a cron expression that can represent fixed times, dates, or intervals.<br/>" +
                "We support cron expressions which consist of 5 <span style='color: #B9F4B7'>Fields</span>.<br/>" +
                "Each field can contain any of the following <span style='color: #f9d291'>Values</span> along with " +
                "various combinations of <span style='color: white'>Special Characters</span> for that field.<br/>" + 
                "<pre>" +
                "+----------------> minute (<span class='values'>0 - 59</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                "|  +-------------> hour (<span class='values'>0 - 23</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                "|  |  +----------> day of month (<span class='values'>1 - 31</span>) (<span class='special-characters'>, - * ? / L W</span>)<br/>" +
                "|  |  |  +-------> month (<span class='values'>1-12 or JAN-DEC</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                "|  |  |  |  +----> day of week (<span class='values'>0-6 or SUN-SAT</span>) (<span class='special-characters'>, - * ? / L #</span>)<br/>" +
                "|  |  |  |  |<br/>" +
                "<span style='color: #B9F4B7'>" +
                "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                "<small><i class='icon-star-filled'></i></small>" +
                "</span></pre><br/>" +
                "For more information see: <a href='http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html' target='_blank'>CronTrigger Tutorial</a></div>"
        });
    }

    savePeriodicBackup() {
        if (!this.validate()) {
             return;
        }

        const dto = this.configuration().toDto();

        new savePeriodicBackupConfigurationCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => this.goToOngoingTasksView());
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.validationGroup)) {
            return;
        }

        bs.isTestingCredentials(true);
        new testPeriodicBackupCredentialsCommand(this.activeDatabase(), bs.connectionType, bs.toDto())
            .execute()
            .always(() => bs.isTestingCredentials(false));
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.configuration().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().localSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().s3Settings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().azureSettings().validationGroup))
            valid = false;

        if (!this.isValid(this.configuration().glacierSettings().validationGroup))
            valid = false;

        return valid;
    }
}

export = editPeriodicBackupTask;