import sys
import os
import glob
import shutil
import psutil
import boto3
import time
from caldp import process
from caldp import log
from caldp import file_ops


class Logs:
    def __init__(self, output_path, output_uri, dataset):
        self.output_path = output_path
        self.output_uri = output_uri
        self.dataset = dataset

    def get_log_output(self, local=True):
        if local is True:
            if self.output_uri.startswith("file"):
                log_output = os.path.join(self.output_path, "logs")
                os.makedirs(log_output, exist_ok=True)
            else:
                log_output = get_local_outpath(self.output_uri, self.dataset)
        elif local is False:
            log_output = self.output_path
        return log_output

    def findlogs(self, log_output):
        proc = list(glob.glob(f"{os.getcwd()}/process*.txt"))
        prev = list(glob.glob(f"{os.getcwd()}/preview*.txt"))
        log_source = proc + prev
        filenames = [os.path.basename(log) for log in log_source]
        log_dest = [log_output + "/" + name for name in filenames]
        return dict(zip(log_source, log_dest))

    def copy_logs(self):
        log_output = self.get_log_output(local=True)
        log_dict = self.findlogs(log_output)
        log.info("Saving log files...")
        for k, v in log_dict.items():
            try:
                shutil.copy(k, v)
                log.info(f"\t{v}.")
            except FileExistsError:
                pass
        log.info("Log files saved.")

    def upload_logs(self):
        log_output = self.get_log_output(local=False)
        log_dict = self.findlogs(log_output)
        client = boto3.client("s3")
        parts = self.output_path[5:].split("/")
        bucket, objectname = parts[0], "/".join(parts[1:])
        log.info("Uploading log files...")
        for k, v in log_dict.items():
            obj = objectname + "/" + os.path.basename(v)
            with open(k, "rb") as f:
                log.info(f"\t Uploading {k} to s3://{bucket}/{obj}.")
                client.upload_fileobj(f, bucket, obj)
                # os.remove(k)
        log.info("Log files uploaded.")


class Messages:
    """Create and update message file according to processing status
    stat : integer value denoting the status
    name : status (string) used for naming the message file based on stat
    file : full path the message file, named according to status
    All message files are updated in place inside the messages directory.
    If s3 is the output_uri, the messages will be update on s3 as well.

    When stat is incremented, the status (filename) updates
    0: None (default state)
    1: 'submit-dataset'
    2: 'processing-dataset'
    3: 'processed-dataset.trigger'
    -1: 'error-dataset'
    """

    def __init__(self, output_uri, output_path, dataset):
        self.output_uri = output_uri
        self.output_path = output_path
        self.dataset = dataset
        self.msg_dir = os.path.join(os.getcwd(), "messages")
        self.stat = 0
        self.name = None
        self.file = None

    def clear_messages(self):
        previous_files = [
            f"error-{self.dataset}",
            f"processing-{self.dataset}",
            f"processed-{self.dataset}.trigger",
        ]
        for f in previous_files:
            self.remove_message(f)

    def init(self):
        os.makedirs(self.msg_dir, exist_ok=True)
        self.clear_messages()
        if self.stat == 0:
            self.name = f"submit-{self.dataset}"
            self.file = f"{self.msg_dir}/{self.name}"
            # self.write_message()  # submit-xxx is written by CALCLOUD
            self.stat += 1  # increment status
        return self

    def process_message(self):
        last_file = self.file
        if self.stat == 1:
            self.name = f"processing-{self.dataset}"
            self.file = f"{self.msg_dir}/{self.name}"
            self.write_message()
            self.remove_message(last_file)
            self.stat += 1
            self.upload_message()
            return self

    def preview_message(self):
        self.stat = 2
        self.name = f"processing-{self.dataset}"
        self.file = f"{self.msg_dir}/{self.name}"
        return self

    def final_message(self):
        if self.stat == 2:
            for _, _, files in os.walk(os.getcwd()):
                for f in files:
                    if f == "process_metrics.txt":
                        proc_metrics = os.path.abspath(f)
                        continue
                    if f == "preview_metrics.txt":
                        prev_metrics = os.path.abspath(f)

            with open(proc_metrics) as proc:
                proc_stat = proc.readlines()[-1].strip("\t").strip("\n")[-1]
            with open(prev_metrics) as prev:
                prev_stat = prev.readlines()[-1].strip("\t").strip("\n")[-1]

            result = int(proc_stat) + int(prev_stat)

            if result > 0:
                self.name = f"error-{self.dataset}"
                self.stat = -1
            else:
                self.name = f"processed-{self.dataset}.trigger"
                self.stat += 1

            last_file = self.file
            self.file = f"{self.msg_dir}/{self.name}"
            self.write_message()
            self.remove_message(last_file)
            self.sync_dataset()
            self.upload_message()
            return self

    def write_message(self):
        with open(self.file, "w") as m:
            m.write(f"{self.name}\n")
            log.info(f"Message file created: {os.path.abspath(self.file)}")

    def remove_message(self, last_file):
        if self.output_uri is None:
            return
        elif os.path.exists(last_file):
            os.remove(last_file)
        if self.output_uri.startswith("s3"):
            obj = os.path.basename(last_file)
            s3 = boto3.resource("s3")  # client = boto3.client('s3')
            bucket = self.output_uri[5:].split("/")[0]
            key = f"messages/{obj}"
            s3.Object(bucket, key).delete()  # client.delete_object(Bucket='mybucketname', Key='myfile.whatever')

    def upload_message(self):
        if self.output_uri.startswith("s3"):
            client = boto3.client("s3")
            bucket = self.output_uri[5:].split("/")[0]
            objectname = f"messages/{self.name}"
            log.info("Uploading message file...")
            with open(self.file, "rb") as f:
                client.upload_fileobj(f, bucket, objectname)
            log.info(f"\ts3://{bucket}/{objectname}")
            log.info("Message file uploaded.")

    def sync_dataset(self):
        if self.output_uri.startswith("file"):
            output_dir = file_ops.get_output_dir(self.output_uri)
            working_dir = os.getcwd()
            os.chdir(output_dir)
            output_files = file_ops.find_output_files(self.dataset)
            outputs = file_ops.find_previews(self.dataset, output_files)
            os.chdir(working_dir)
            # preview_output = os.path.join(self.output_path, "previews")
            # files = glob.glob(f"{self.output_path}/{self.dataset[0:5]}*")
            # files.extend(glob.glob(f"{preview_output}/{self.dataset[0:5]}*"))
            # outputs = list(sorted(files))
            for line in outputs:
                with open(self.file, "a") as m:
                    m.write(f"{line}\n")
            log.info(f"Dataset synced: {outputs}")

        elif self.output_uri.startswith("s3"):
            s3_path = f"{self.output_path}/{self.dataset}.tar.gz"
            with open(self.file, "w") as m:
                m.write(s3_path)
            log.info(f"Dataset synced: {s3_path}")


# for pytest cov (create metrics files similar to caldp-process bash script)
def log_metrics(log_file, metrics):
    res = {}
    res["walltime"] = time.time()
    res["clocktime"] = time.process_time()
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


def clean_up(dataset, IO):
    print(f"Cleaning up {IO}...")
    folder = os.path.join(os.getcwd(), IO)
    shutil.rmtree(folder, ignore_errors=True)
    print("Done.")
    # if IO == "messages":
    #     file_list = list(glob.glob(f"{folder}/*"))
    #     for f in file_list:
    #         os.remove(f)
    # else:
    #     ipst = os.path.join(folder, ipppssoot)
    #     file_list = list(glob.glob(f"{ipst}/*"))
    #     for f in file_list:
    #         if os.path.isfile(f):
    #             os.remove(f)
    #         elif os.path.isdir(f):
    #             if len(os.listdir(f)) > 0:
    #                 shutil.rmtree(f)
    #             else:
    #                 os.rmdir(f)
    #     os.rmdir(ipst)
    # os.rmdir(folder)
    # print("Done.")


# primarily for test cov where output_uri is "none"
def path_finder(input_uri, output_uri_prefix, dataset):
    if output_uri_prefix is None:
        if input_uri.startswith("file"):
            output_uri = input_uri
            output_dir = output_uri.split(":")[-1] or "."
            output_path = os.path.abspath(output_dir)
        else:
            output_dir = os.path.join(os.getcwd(), "inputs", dataset)
            output_uri = f"file:{output_dir}"
            output_path = os.path.abspath(output_dir)
    else:
        output_uri = output_uri_prefix
        output_path = process.get_output_path(output_uri, dataset)
    return output_uri, output_path


# find where to put logs
def get_local_outpath(output_uri, dataset):
    """Returns full path to folder containing output files."""
    if output_uri.startswith("s3"):
        local_outpath = process.get_output_path("file:outputs", dataset)
    else:
        local_outpath = process.get_output_path(output_uri, dataset)
    return local_outpath


def main(input_uri, output_uri_prefix, dataset):
    """This function is designed to run after calibration has completed."""
    output_uri, output_path = path_finder(input_uri, output_uri_prefix, dataset)
    logs = Logs(output_path, output_uri, dataset)
    logs.copy_logs()
    msg = Messages(output_uri, output_path, dataset)
    msg.preview_message()
    msg.final_message()
    if output_uri.startswith("s3"):
        logs.upload_logs()
        clean_up(dataset, IO="outputs")
        clean_up(dataset, IO="messages")
        if not input_uri.startswith("file"):
            clean_up(dataset, IO="inputs")


def cmd(argv):
    """Top level function, process args <input_uri> <output_uri>  <dataset>"""
    input_uri = str(argv[1])
    output_uri_prefix = str(argv[2])
    dataset = str(argv[3])
    if output_uri_prefix.lower().startswith("none"):
        output_uri_prefix = None
    main(input_uri, output_uri_prefix, dataset)


if __name__ == "__main__":
    cmd(sys.argv)
