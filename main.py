import json
import logging
from datetime import datetime

import boto3


class SnapshotManagement(object):

    def __init__(self, event):
        self.logger = self.__get_logger()

        self.start_of_day = 19  # Means 2am UTC+7
        self.start_of_week = "Monday"  # Means Tuesday on UTC+7
        self.start_of_month = 1  # 1st day on each month
        self.start_of_year = 1  # January

        self.product_domain = event["ProductDomain"]
        self.service = event["Service"]
        self.cluster = event["Cluster"]
        self.application = event["Application"]
        self.retention_policy = event["RetentionPolicy"]
        self.volume_ids = event["VolumeIDs"].split(",")

        self.ec2_client = boto3.client("ec2")
        self.ec2_resource = boto3.resource("ec2")
        self.snapshots = self.__get_all_snapshots()

    @staticmethod
    def __get_logger():
        formatter = logging.Formatter('%(levelname)s Message: "%(message)s"')

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        return logger

    def __log(self, log_level, volume_id, message):
        full_message = "{} - {} - {}  - {} - {}".format(
            self.product_domain,
            self.service,
            self.cluster,
            volume_id,
            message
        )
        self.logger.log(level=log_level, msg=full_message)

    def __get_all_snapshots(self):
        status = True

        ret = {}
        for volume_id in self.volume_ids:
            try:
                snapshots = self.ec2_client.describe_snapshots(
                    Filters=[
                        {
                            "Name": "volume-id",
                            "Values": [
                                volume_id
                            ]
                        },
                        {
                            "Name": "status",
                            "Values": [
                                "pending",
                                "completed"
                            ]
                        },
                    ],
                )
                ret[volume_id] = snapshots["Snapshots"]
                self.__log(
                    log_level=logging.INFO,
                    volume_id=volume_id,
                    message="Snapshots Acquired"
                )
            except Exception as e:
                status = False
                self.__log(
                    log_level=logging.ERROR,
                    volume_id=volume_id,
                    message=e
                )

        if not status:
            exit(429)
        else:
            return ret

    @staticmethod
    def __get_snapshots_by_state(snapshots, state):
        return list([x for x in snapshots if x["State"] == state])

    @staticmethod
    def __get_snapshots_by_backup_tag(snapshots, backup_tag):
        return [
            x
            for x in [x for x in [x for x in snapshots if "Tags" in x] if "BackupTag" in [y["Key"] for y in x["Tags"]]]
            for y in x["Tags"]
            if y["Key"] == "BackupTag" and y["Value"] == backup_tag
        ]

    @staticmethod
    def __get_oldest_snapshot_ids(snapshots, number_of_snapshots):
        snapshots = [x["SnapshotId"] for x in sorted(
            snapshots, key=lambda x: x["StartTime"], reverse=True)]
        return list(snapshots[-number_of_snapshots:])

    def __get_backup_tag(self):
        current_datetime = datetime.now()
        datetime_dict = {
            "dayname": current_datetime.strftime("%A"),
            "m": current_datetime.month,
            "d": current_datetime.day,
            "h": current_datetime.hour,
        }

        if (
                datetime_dict["h"] == self.start_of_day and
                datetime_dict["d"] == self.start_of_month and
                datetime_dict["m"] == self.start_of_year
        ):
            backup_tag = "y"

        elif (
                datetime_dict["h"] == self.start_of_day and
                datetime_dict["d"] == self.start_of_month
        ):
            backup_tag = "m"

        elif (
                datetime_dict["h"] == self.start_of_day and
                datetime_dict["dayname"] == self.start_of_week
        ):
            backup_tag = "w"

        elif (
                datetime_dict["h"] == self.start_of_day
        ):
            backup_tag = "d"

        else:
            backup_tag = "h"
        return backup_tag

    def __delete_old_snapshots(self, volume_id):
        status = True
        retention_policy = json.loads(self.retention_policy)

        ret = []
        for tag in ["h", "d", "w", "m", "y"]:
            snapshots = self.__get_snapshots_by_backup_tag(
                snapshots=self.snapshots[volume_id], backup_tag=tag
            )

            snapshot_ids = []
            if tag in retention_policy:
                diff = len(snapshots) - retention_policy[tag]
                if diff > 0:
                    snapshot_ids = self.__get_oldest_snapshot_ids(
                        snapshots=snapshots, number_of_snapshots=diff
                    )
            else:
                if len(snapshots) > 0:
                    snapshot_ids = self.__get_oldest_snapshot_ids(
                        snapshots=snapshots, number_of_snapshots=0
                    )

            for snapshot_id in snapshot_ids:
                try:
                    self.ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                    ret.append(snapshot_id)
                    self.__log(
                        log_level=logging.INFO,
                        volume_id=volume_id,
                        message="Snapshot Deleted: {}".format(snapshot_id)
                    )
                except Exception as e:
                    status = False
                    message = "Failed to Delete Snapshot {}: {}".format(
                        snapshot_id, e
                    )
                    ret.append(message)
                    self.__log(
                        log_level=logging.ERROR,
                        volume_id=volume_id,
                        message=message
                    )
        return status, ret

    def __create_snapshot(self, volume_id):
        status = True
        backup_tag = self.__get_backup_tag()
        volume = self.ec2_resource.Volume(volume_id)
        try:
            snapshot = volume.create_snapshot(
                TagSpecifications=[
                    {
                        "ResourceType": "snapshot",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": self.cluster
                            },
                            {
                                "Key": "ProductDomain",
                                "Value": self.product_domain
                            },
                            {
                                "Key": "Service",
                                "Value": self.service
                            },
                            {
                                "Key": "Cluster",
                                "Value": self.cluster
                            },
                            {
                                "Key": "Application",
                                "Value": self.application
                            },
                            {
                                "Key": "BackupTag",
                                "Value": backup_tag
                            }
                        ]
                    }
                ]
            )
            self.__log(
                log_level=logging.INFO,
                volume_id=volume_id,
                message="Snapshot Created: {}".format(snapshot.id)
            )
            return status, snapshot.id
        except Exception as e:
            status = False
            message = "Failed to Create Snapshot: {}".format(e)
            self.__log(
                log_level=logging.ERROR,
                volume_id=volume_id,
                message=message
            )
            return status, message

    def execute(self):
        final_status = True
        ret = {
            "created": {},
            "deleted": {}
        }
        for volume_id, snapshots in list(self.snapshots.items()):
            pending_snapshots = self.__get_snapshots_by_state(
                snapshots=snapshots, state="pending"
            )
            if not pending_snapshots:
                status, retval = self.__create_snapshot(volume_id=volume_id)
            else:
                message = "Pending Snapshot Detected, Operation Aborted"
                self.__log(
                    log_level=logging.WARNING,
                    volume_id=volume_id,
                    message=message
                )
                status, retval = True, message
            ret["created"][volume_id] = retval

            final_status = final_status and status

            status, retval = self.__delete_old_snapshots(volume_id=volume_id)
            ret["deleted"][volume_id] = retval

            final_status = final_status and status
        return final_status, ret


def lambda_handler(event, context):
    snapshot_management = SnapshotManagement(event=event)
    status, retval = snapshot_management.execute()

    print((json.dumps(retval)))
    if status:
        return retval
    else:
        exit(-1)
