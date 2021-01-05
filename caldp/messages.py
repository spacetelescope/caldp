import sys
import os
import glob
import shutil
import psutil
import boto3
import time
from caldp import process
from caldp import log


class Logs:
    def __init__(self, output_path, output_uri):
        self.output_path = output_path
        self.output_uri = output_uri
        self.log_output = os.path.join(self.output_path, "logs")

    def __repr__(self):
        return str(self.log_output)

    def findlogs(self):
        search_log = f"{os.getcwd()}/*.txt"
        log_source = list(glob.glob(search_log))
        filenames = [os.path.basename(log) for log in log_source]
        log_dest = [self.log_output + "/" + name for name in filenames]
        return dict(zip(log_source, log_dest))

    def copy_logs(self):
        log_dict = self.findlogs()
        os.makedirs(self.log_output, exist_ok=True)
        log.info("Saving log files...")
        for k, v in log_dict.items():
            try:
                shutil.copy(k, v)
                log.info(f"\t{v}.")
            except FileExistsError:
                pass
        log.info("Log files saved.")

    def upload_logs(self):
        log_dict = self.findlogs()
        client = boto3.client("s3")
        parts = self.log_output[5:].split("/")
        bucket, objectname = parts[0], "/".join(parts[1:])
        log.info("Uploading log files...")
        for k, v in log_dict.items():
            obj = objectname + "/" + os.path.basename(v)
            with open(k, "rb") as f:
                client.upload_fileobj(f, bucket, obj)
                log.info(f"\t{v}.")
        log.info("Log files uploaded.")


class Messages:
    def __init__(self, output_uri, output_path, ipppssoot):
        self.output_uri = output_uri
        self.output_path = output_path
        self.ipppssoot = ipppssoot

    def status_check(self):
        cwd = os.getcwd()
        for _, _, files in os.walk(cwd):
            for f in files:
                if f.endswith("process_metrics.txt"):
                    process_metrics = os.path.abspath(f)
                    continue
                if f.endswith("preview_metrics.txt"):
                    preview_metrics = os.path.abspath(f)
        with open(process_metrics) as f:
            proc_stat = f.readlines()[-1].strip("\t").strip("\n")[-1]
        with open(preview_metrics) as f:
            prev_stat = f.readlines()[-1].strip("\t").strip("\n")[-1]
        status = int(proc_stat) + int(prev_stat)
        if status > 0:
            stat = "dataset-error"
        else:
            stat = "dataset-processed"
        return stat

    def make_messages(self, stat):
        base = os.getcwd()
        msg_dir = os.path.join(base, "messages", stat)
        os.makedirs(msg_dir, exist_ok=True)
        msg_file = msg_dir + f"/{self.ipppssoot}"
        with open(msg_file, "w") as m:
            m.write(f"{stat} {self.ipppssoot}\n")
            log.info(f"Message file saved: {os.path.abspath(msg_file)}")
        return os.path.abspath(msg_file)

    def upload_messages(self, stat, msg_file):
        client = boto3.client("s3")
        parts = self.output_uri[5:].split("/")
        bucket = parts[0]
        if len(parts) > 1:
            objectname = parts[1] + f"/messages/{stat}/{self.ipppssoot}"
        else:
            objectname = f"messages/{stat}/{self.ipppssoot}"
        log.info("Uploading message file...")
        with open(msg_file, "rb") as f:
            client.upload_fileobj(f, bucket, objectname)
        log.info(f"\ts3://{bucket}/{objectname}")
        log.info("Message file uploaded.")

    def sync_dataset(self, stat):
        preview_output = os.path.join(self.output_path, "previews")
        files = glob.glob(f"{self.output_path}/{self.ipppssoot[0:5]}*")
        files.extend(glob.glob(f"{preview_output}/{self.ipppssoot[0:5]}*"))
        outputs = list(sorted(files))

        if stat == "dataset-processed":
            base = os.getcwd()
            sync_dir = os.path.join(base, "messages", "dataset-synced")
            os.makedirs(sync_dir, exist_ok=True)
            sync_msg = sync_dir + "/" + self.ipppssoot
            for line in outputs:
                with open(sync_msg, "a") as m:
                    m.write(f"{line}\n")
            log.info(f"Dataset synced: {os.path.abspath(sync_msg)}")
        else:
            log.error("Error found - skipping data sync.")


def log_metrics(log_file, metrics):
    res = {}
    res["walltime"] = time.time()
    res["clocktime"] = time.clock()
    res["cpu"] = psutil.cpu_percent(interval=None)
    res["memory"] = psutil.virtual_memory().percent
    res["swap"] = psutil.swap_memory().percent
    res["disk"] = psutil.disk_usage(log_file).percent
    res["Exit status"] = "0"

    with open(metrics, "w") as f:
        for k, v in res.items():
            f.write(f"{k}: {v}\n")
    print(os.path.abspath(metrics))

    return res


def path_finder(input_uri, output_uri_prefix, ipppssoot):
    if output_uri_prefix.lower().startswith("none"):
        if input_uri.startswith("file"):
            output_uri = input_uri
            output_dir = output_uri.split(":")[-1] or "."
            output_path = os.path.abspath(output_dir)
        else:
            output_dir = os.path.join(os.getcwd(), ipppssoot)
            output_uri = f"file:{output_dir}"
            output_path = os.path.abspath(output_dir)
    else:
        output_uri = output_uri_prefix
        output_path = process.get_output_path(output_uri, ipppssoot)
    return output_uri, output_path


def main(input_uri, output_uri_prefix, ipppssoot):
    output_uri, output_path = path_finder(input_uri, output_uri_prefix, ipppssoot)
    logs = Logs(output_path, output_uri)
    msg = Messages(output_uri, output_path, ipppssoot)
    stat = msg.status_check()
    msg_file = msg.make_messages(stat)
    if output_uri.startswith("s3"):
        logs.upload_logs()
        msg.upload_messages(stat, msg_file)
    else:
        logs.copy_logs()
        msg.sync_dataset(stat)


def cmd(argv):
    """Top level function, process args <input_uri> <output_uri>  <ipppssoot>"""
    input_uri = str(argv[1])
    output_uri_prefix = str(argv[2])
    ipppssoot = str(argv[3])
    main(input_uri, output_uri_prefix, ipppssoot)


if __name__ == "__main__":
    cmd(sys.argv)
