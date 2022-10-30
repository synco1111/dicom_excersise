import os.path
import pathlib
import urllib.request
import tarfile
import shutil

# dicom modules
from pydicom.filereader import dcmread
import pandas as pd
from tabulate import tabulate


class Patient:
    def __init__(
        self,
        patient_name,
        patient_age,
        patient_sex,
        study_instance_uid,
        series_instance_uid,
        institute_name,
        exposure_time,
        file_name,
    ):
        self.patient_name = patient_name
        self.patient_age = patient_age
        self.patient_sex = patient_sex
        self.study_instance_uid = study_instance_uid
        self.series_instance_uid = series_instance_uid
        self.institute_name = institute_name
        self.exposure_time = exposure_time
        self.file_name = file_name


class Utils:
    @staticmethod
    def to_csv(df, file_name: str):
        if os.path.isfile(file_name):
            print(f"File {file_name} already exists")
        else:
            df.to_csv(file_name)

    @staticmethod
    def download_from_url(dicom_url):
        file_name = dicom_url.split("/")[-1]
        # Downloading file if not exists
        if os.path.isfile(file_name):
            print(f"File {file_name} already exists")
        else:
            print(f"downloading file from url {dicom_url}")
            urllib.request.urlretrieve(dicom_url, file_name)
        return file_name

    @staticmethod
    def extract_tar(file_name, extract_path):
        print(f"Extracting dicom files into folder: {os.path.abspath(extract_path)}")
        file = tarfile.open(file_name)
        if os.path.isdir(extract_path):
            print(f"Dicom files already extract")
        else:
            file.extractall(extract_path)
            print("Extraction completed")
        dicom_file_list = file.getnames()
        file.close()
        return dicom_file_list


if __name__ == "__main__":
    if not (os.path.isfile("patients_metadata.csv") and os.path.isfile("DM_TH.tgz")):
        utils = Utils()
        url = "https://s3.amazonaws.com/viz_data/DM_TH.tgz"
        file_name = utils.download_from_url(url)

        # Extract files if not exist
        extract_path = "dicom_files"
        dicom_file_list = utils.extract_tar(file_name, extract_path)

        # Handle file
        dicom_parse_data = []
        for dicom_file in os.listdir(extract_path):
            f = os.path.join(extract_path, dicom_file)
            ds = dcmread(os.path.abspath(f))
            patient = {}
            patient = Patient(
                ds.PatientName,
                ds.PatientAge,
                ds.PatientSex,
                ds.StudyInstanceUID,
                ds.SeriesInstanceUID,
                ds.InstitutionName,
                ds.ExposureTime,
                dicom_file,
            )
            dicom_parse_data.append(patient)

        df = pd.DataFrame.from_records([vars(s) for s in dicom_parse_data])
        # print(tabulate(df, headers="keys", tablefmt="psql"))
        Utils.to_csv(df, "patients_metadata.csv")
    else:
        df = pd.read_csv("patients_metadata.csv")

    # Files arranging
    rootdir = pathlib.Path("./dicom_report")

patient_structure = df.apply(
    lambda x: rootdir
    / "patient_name"
    / x["patient_name"]
    / "Study"
    / x["study_instance_uid"]
    / "Series"
    / x["series_instance_uid"]
    / x["file_name"],
    axis="columns",
)

df_length = len(df.index)
counter = 1

for csvfile, data in df.groupby(patient_structure):

    csvfile.parent.mkdir(parents=True, exist_ok=True)
    dicom_filename = str(csvfile).split("\\")[-1]
    # data.to_csv(csvfile, index=False)
    # create dir
    csvfile.parent.mkdir(parents=True, exist_ok=True)
    # copy file to destination
    src = f"./dicom_files/{dicom_filename}"
    dst = "/".join(str(csvfile).split("\\")[:7])

    print(f"Files Proceesed {counter}/{df_length }")
    shutil.copy(src, dst)
    counter += 1

print("Patient files arranging completed.")
