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
###fix metadata path and set it as an argument
###use this path:/CCR_SCAF_Archive/Anish_Thomas_lab/CS029608_Schultz/Analysis/
###make a metadata file for project folder as well

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
def convert_metadata_json_to_dict(metadata_json):
	"""
	"""
	f = open(metadata_json) 
	
	json_Dict = json.load(f)
	
	return json_Dict

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
def build_analysis_metadata(analysis_directory, new_analysis_file_path, metadata_file_path, platform):
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


def build_project_name_metadata(destination_path, project_name, top_metadata):
	"""
	"""
	metadata_file_path = destination_path + "/" + project_name + ".metadata.json"
	# metadata_ = json.dumps(top_metadata)

	with open(metadata_file_path, 'w', encoding='utf-8') as f:
		json.dump(top_metadata, f, ensure_ascii=False, indent=4)

	return True

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
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

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
def build_fastq_metadata(fastq_directory, new_fastq_file_path, metadata_file_path, platform):
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

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
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

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
def build_new_structure(scaf_sample_Dict, destination_path, hpc_dme_path, platform, project_name, top_metadata):
	"""
	"""
	if not os.path.exists(destination_path):
		os.makedirs(destination_path)
	build_project_name_metadata(destination_path, project_name, top_metadata)

	destination_directory = destination_path + "/" + project_name

	for each_sample in scaf_sample_Dict:

		if each_sample == "result":
			#
			result_directory = destination_directory + "/Analysis"
			hpc_dme_result_directry = hpc_dme_path + "/Analysis"
			if not os.path.exists(result_directory):
				os.makedirs(result_directory)
			else:
				pass
			for each_result in scaf_sample_Dict["result"]:

				result_file = each_result.split("/")[-1]
				new_result_file_path = result_directory + "/" + result_file + ".tar"
				hpc_dme_result_file_Path = hpc_dme_result_directry + "/" + result_file + ".tar"
				Path(new_result_file_path).touch()
				metadata_json_file_path = new_result_file_path + ".metadata.json"
				build_result_metadata(hpc_dme_result_directry, hpc_dme_result_file_Path, metadata_json_file_path)

			else:
				pass
		elif each_sample == "excel_file":
			#
			result_directory = destination_directory + "/Analysis"
			hpc_dme_result_directry = hpc_dme_path + "/Analysis"
			if not os.path.exists(result_directory):
				os.makedirs(result_directory)
			else:
				pass
			for each_excel in scaf_sample_Dict["excel_file"]:
				##
				each_excel_file = os.path.basename(each_excel)
				new_excel_file_path = result_directory + "/" + each_excel_file
				hpc_dme_excel_file_Path = hpc_dme_result_directry + "/" + each_excel_file
				shutil.copyfile(each_excel, new_excel_file_path)
				metadata_json_file_path = new_excel_file_path + ".metadata.json"
				build_result_metadata(hpc_dme_result_directry, hpc_dme_excel_file_Path, metadata_json_file_path)
		else:
			#
			sample_directory = destination_directory + "/" + each_sample
			fastq_directory = destination_directory + "/" + each_sample + "/FASTQ"
			hpc_dme_fastq_directry = hpc_dme_path + "/" + each_sample + "/FASTQ"
			primary_analysis_directory = destination_directory + "/" + each_sample + "/Primary_Analysis_Output"
			hpc_dme_primary_analysis_directry = hpc_dme_path + "/" + each_sample + "/Primary_Analysis_Output"
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
					hc_dme_new_fastq_file_path = hpc_dme_fastq_directry + "/" + fastq_file_name + "_FQ_" + each_flowcell + ".tar"
					Path(new_fastq_file_path).touch()
					metadata_json_file_path = new_fastq_file_path + ".metadata.json"
					build_fastq_metadata(hpc_dme_fastq_directry, hc_dme_new_fastq_file_path, metadata_json_file_path, platform)
			else:
				pass
			for each_analysis_type in scaf_sample_Dict[each_sample]["analysis"]:
				##
				for each_analysis in scaf_sample_Dict[each_sample]["analysis"][each_analysis_type]:
					analysis_file_name = os.path.basename(each_analysis).split(".tar")[0]
					new_analysis_file_path = primary_analysis_directory + "/" + analysis_file_name + "_PA_" + each_analysis_type + ".tar"
					hc_dme_new_analysis_file_path = hpc_dme_primary_analysis_directry + "/" + analysis_file_name + "_PA_" + each_analysis_type + ".tar"
					Path(new_analysis_file_path).touch()
					metadata_json_file_path = new_analysis_file_path + ".metadata.json"
					build_analysis_metadata(hpc_dme_primary_analysis_directry, hc_dme_new_analysis_file_path, metadata_json_file_path, platform)
			else:
				pass
	else:
		pass
	print("Done")
	return True

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
metadata_json = sys.argv[1]
metadata_Dict = convert_metadata_json_to_dict(metadata_json)
source_directory = metadata_Dict["source_directory"]
destination_path = metadata_Dict["destination_path"]
hpc_dme_path = metadata_Dict["hpc_dme_path"]
platform = metadata_Dict["platform"]
project_name = metadata_Dict["project_name"]
top_metadata = metadata_Dict["top_metadata"]
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
scaf_sample_Dict = build_scaf_sample_Dict(source_directory)	
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
build_new_structure(scaf_sample_Dict, destination_path, hpc_dme_path, platform, project_name, top_metadata)
print("recast is completed")
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	

