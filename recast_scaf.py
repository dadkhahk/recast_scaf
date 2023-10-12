import os
import sys
import argparse
import glob
from pathlib import Path
import shutil
import json

"""
python recast_scaf.py /data/dadkhahe/HPC_DME_Example/CS029608_Schultz ./try2 "platform"
"""

def build_analysis_metadata(analysis_directory, new_analysis_file_path, metadata_file_path):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": os.path.abspath(new_analysis_file_path)
		},
		{
			"attribute": "source_path",
			"value": os.path.abspath(analysis_directory) + "/"
		},
		{
			"attribute": "platform_name",
			"value": platform
		}
	]
	metadata_Json = json.dumps(metadata_Dict)

	with open(metadata_file_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_result_metadata(result_directory, new_result_file_path, metadata_file_path):
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": os.path.abspath(new_result_file_path)
		},
		{
			"attribute": "source_path",
			"value": os.path.abspath(result_directory) + "/"
		}
	]
	metadata_Json = json.dumps(metadata_Dict)

	with open(metadata_file_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_fastq_metadata(fastq_directory, new_fastq_file_path, metadata_file_path):
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": os.path.abspath(new_fastq_file_path)
		},
		{
			"attribute": "source_path",
			"value": os.path.abspath(fastq_directory) + "/"
		},
		{
			"attribute": "platform_name",
			"value": platform
		}
	]
	metadata_Json = json.dumps(metadata_Dict)

	with open(metadata_file_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_scaf_sample_Dict(source_directory):
	"""
	"""
	# ++++++++++++++
	scaf_sample_Dict = {}
	fastq_List = glob.glob( source_directory + "/01_DemultiplexedFastqs/**/*.tar", recursive=True)
	for each_fastq in fastq_List:
		##
		fastq_name = os.path.basename(each_fastq)
		flowcell_ID = os.path.dirname(each_fastq).split("/")[-1].split("_", 1)[1]
		sample_name = fastq_name.split(".tar")[0]
		if sample_name not in scaf_sample_Dict:
			#
			scaf_sample_Dict[sample_name] = {}
			scaf_sample_Dict[sample_name]["fastq"] = {}
			scaf_sample_Dict[sample_name]["analysis"] = {}
			
			scaf_sample_Dict[sample_name]["fastq"][flowcell_ID] = [each_fastq]
		elif flowcell_ID not in scaf_sample_Dict[sample_name]["fastq"]:
			scaf_sample_Dict[sample_name]["fastq"][flowcell_ID] = [each_fastq]
		else:
			scaf_sample_Dict[sample_name][flowcell_ID].append(each_fastq)
	else:
		pass
	# +++++++++++++++++
	analysis_List = glob.glob( source_directory + "/02_PrimaryAnalysisOutput/00_FullCellrangerOutputs/**/*.tar", recursive=True)
	for each_analysis in analysis_List:
		##
		analysis_name = os.path.basename(each_analysis)
		analysis_type = os.path.dirname(each_analysis).split("/")[-1]
		sample_name = analysis_name.split(".tar")[0]
		if sample_name not in scaf_sample_Dict:
			#
			scaf_sample_Dict[sample_name] = {}
			scaf_sample_Dict[sample_name]["fastq"] = {}
			scaf_sample_Dict[sample_name]["analysis"] = {}
			
			scaf_sample_Dict[sample_name]["analysis"][analysis_type] = [each_analysis]
		elif analysis_type not in scaf_sample_Dict[sample_name]["analysis"]:
			scaf_sample_Dict[sample_name]["analysis"][analysis_type] = [each_analysis]
		else:
			scaf_sample_Dict[sample_name]["analysis"][analysis_type].append(each_analysis)
	else:
		pass
	# +++++++++++++++++
	scaf_sample_Dict["result"] = []
	result_List = ["01_SummaryHTMLs", "02_LoupeCellBrowserFiles", "03_FilteredMatricesH5", "04_RawMatricesH5"]
	for each_result in result_List:
		##
		each_result_path = source_directory + "/02_PrimaryAnalysisOutput/" + each_result
		if os.path.exists(each_result_path) is True:
			scaf_sample_Dict["result"].append(each_result_path)
		else:
			pass
	else:
		pass
	# +++++++++++++++++
	#for execl file
	scaf_sample_Dict["excel_file"] = glob.glob( source_directory + "/*.xlsx", recursive=True)
	

	return scaf_sample_Dict


def build_new_structure(scaf_sample_Dict, destination_directory):
	"""
	"""
	for each_sample in scaf_sample_Dict:

		if each_sample == "result":
			#
			result_directory = destination_directory + "/Analysis"
			if not os.path.exists(result_directory):
				os.makedirs(result_directory)
			else:
				pass
			for each_result in scaf_sample_Dict["result"]:

				result_file = each_result.split("/")[-1]
				new_result_file_path = result_directory + "/" + result_file + ".tar"
				Path(new_result_file_path).touch()
				metadata_json_file_path = new_result_file_path + ".metadata.json"
				build_result_metadata(result_directory, new_result_file_path, metadata_json_file_path)

			else:
				pass
		elif each_sample == "excel_file":
			#
			result_directory = destination_directory + "/Analysis"
			if not os.path.exists(result_directory):
				os.makedirs(result_directory)
			else:
				pass
			for each_excel in scaf_sample_Dict["excel_file"]:
				##
				each_excel_file = os.path.basename(each_excel)
				new_excel_file_path = result_directory + "/" + each_excel_file
				shutil.copyfile(each_excel, new_excel_file_path)
				metadata_json_file_path = new_excel_file_path + ".metadata.json"
				build_result_metadata(result_directory, new_excel_file_path, metadata_json_file_path)
		else:
			#
			sample_directory = destination_directory + "/" + each_sample
			fastq_directory = destination_directory + "/" + each_sample + "/FASTQ"
			primary_analysis_directory = destination_directory + "/" + each_sample + "/Primary_Analysis_Output"
			if not os.path.exists(sample_directory):
				os.makedirs(sample_directory)
				os.makedirs(fastq_directory)
				os.makedirs(primary_analysis_directory)
			else:
				pass
			for each_flowcell in scaf_sample_Dict[each_sample]["fastq"]:
				##
				for each_fastq in scaf_sample_Dict[each_sample]["fastq"][each_flowcell]:
					##
					fastq_file_name = os.path.basename(each_fastq).split(".tar")[0]
					new_fastq_file_path = fastq_directory + "/" + fastq_file_name + "_FQ_" + each_flowcell + ".tar"
					Path(new_fastq_file_path).touch()
					metadata_json_file_path = new_fastq_file_path + ".metadata.json"
					build_fastq_metadata(fastq_directory, new_fastq_file_path, metadata_json_file_path)
			else:
				pass
			for each_analysis_type in scaf_sample_Dict[each_sample]["analysis"]:
				##
				for each_analysis in scaf_sample_Dict[each_sample]["analysis"][each_analysis_type]:
					analysis_file_name = os.path.basename(each_analysis).split(".tar")[0]
					new_analysis_file_path = primary_analysis_directory + "/" + analysis_file_name + "_PA_" + each_analysis_type + ".tar"
					Path(new_analysis_file_path).touch()
					metadata_json_file_path = new_analysis_file_path + ".metadata.json"
					build_analysis_metadata(primary_analysis_directory, new_analysis_file_path, metadata_json_file_path)
			else:
				pass
	else:
		pass
	print("Done")
	return True

	
source_directory = sys.argv[1]
destination_directory = sys.argv[2]
platform = sys.argv[3]

scaf_sample_Dict = build_scaf_sample_Dict(source_directory)

# print(scaf_sample_Dict)
# sys.exit(2)
build_new_structure(scaf_sample_Dict, destination_directory)


