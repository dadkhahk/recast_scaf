import os
import sys
import argparse
import glob
from pathlib import Path
import shutil
import json

"""
python recast_scaf.py recast_scaf.json
"""

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++	
def convert_metadata_json_to_dict(metadata_json):
	"""
	"""
	f = open(metadata_json) 
	
	json_Dict = json.load(f)
	
	return json_Dict


def initiate_swarm_file(swarm_file_path, swarm_log_path):
	"""
	"""
	os.makedirs(os.path.dirname(swarm_file_path), exist_ok = True)
	swarm_string = "#swarm -g 10 -t 2 --time 24:00:00 --sbatch --mail-type=ALL --logdir " + swarm_log_path + " -f " + swarm_file_path + "\n"

	f = open(swarm_file_path, 'a')
	f.write(swarm_string)
	f.close()

	return True

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def build_tar_swarm_script(source_directory, file_source_path, file_destination_path, swarm_file_path):
	"""
	tar -cWf /data/dadkhahe/development/test_tar/01_SummaryHTMLs.tar --directory="/data/dadkhahe/HPC_DME_Example/CS029608_Schultz/02_PrimaryAnalysisOutput/" 01_SummaryHTMLs
	"""
	#swarm_script = "tar -cWf " + file_destination_path + " " + file_source_path +  "\n"
	
	swarm_script = 'tar -cWf ' + file_destination_path + ' --directory="' + source_directory + '" ' + os.path.basename(file_source_path) +  ' \n'
	f = open(swarm_file_path, 'a')
	f.write(swarm_script)
	f.close()

	return True


def build_copy_swarm_script(file_source_path, file_destination_path, swarm_file_path):
	"""
	"""
	swarm_script = "cp " + file_source_path + " " + file_destination_path + "\n"
	f = open(swarm_file_path, 'a')
	f.write(swarm_script)
	f.close()

	return True

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def build_sample_json_template(json_Dict, sample_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "sample_id",
			"value": sample_name
		},
		{
			"attribute": "sample_name",
			"value": sample_name
		},
		{
			"attribute": "curation_status",
			"value": "False"
		},
			
		{
			"attribute": "collection_type",
			"value": "Sample"
								},
			{
			"attribute": "data_generator",
			"value": json_Dict["data_generator"]
			
		},
		{
			"attribute": "data_owner_affiliation",
			"value": json_Dict["data_owner_affiliation"]
		},
		{
			"attribute": "data_owner_email",
			"value": json_Dict["data_owner_email"]
								},
		{
			"attribute": "data_owner",
			"value": json_Dict["data_owner"]
								},
							{
			"attribute": "data_generator_email",
			"value": json_Dict["data_generator_email"]
								},
		{
			"attribute": "data_generator_affiliation",
			"value": json_Dict["data_generator_affiliation"]
								},
		{
			"attribute": "project_poc",
			"value": json_Dict["project_poc"]

		},
		{
			"attribute": "project_poc_affiliation",
			"value": json_Dict["project_poc_affiliation"]

		},
		{
			"attribute": "project_poc_email",
			"value": json_Dict["project_poc_email"]

		},
		{
			"attribute": "project_title",
			"value": json_Dict["project_title"]
	   },
		{
			"attribute": "project_id",
			"value":  json_Dict["project_id"]

		},
		{
			"attribute": "access",
			"value": "Controlled Access"

		},
		{
			"attribute": "project_start_date",
			"value": "2021-06-15"
		}
		
	]

	sample_folder_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + sample_name + ".metadata.json"

	with open(sample_folder_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_analysis_json_template(json_Dict, analysis_tar_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/Analysis/" + analysis_tar_name
		},
		{
			"attribute": "source_path",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/Analysis/"
		},
		{
			"attribute": "platform_name",
			"value": json_Dict["platform_name"]
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	analysis_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/Analysis/" + analysis_tar_name + ".metadata.json"
	with open(analysis_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_analysis_excel_json_template(json_Dict, excel_tar_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/Analysis/" + excel_tar_name
		},
		{
			"attribute": "source_path",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/Analysis/"
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	excel_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/Analysis/" + excel_tar_name + ".metadata.json"
	with open(excel_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_analysis_folder_json_template(json_Dict):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
		"attribute": "collection_type",
		"value": "Analysis"
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	analysis_folder_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + "Analysis.metadata.json"
	with open(analysis_folder_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_fastq_folder_json_template(json_Dict, sample_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
		"attribute": "collection_type",
		"value": "FASTQ"
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	fastq_folder_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + sample_name + "/FASTQ.metadata.json"
	with open(fastq_folder_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_fastq_json_template(json_Dict, sample_name, fastq_tar_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/" + sample_name + "/FASTQ/" + fastq_tar_name
		},
		{
			"attribute": "source_path",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/" + sample_name + "/FASTQ/"
		},
		{
			"attribute": "platform_name",
			"value": json_Dict["platform_name"]
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	fastq_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + sample_name + "/FASTQ/" + fastq_tar_name + ".metadata.json"
	with open(fastq_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_primary_folder_json_template(json_Dict, sample_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
		"attribute": "collection_type",
		"value": "Primary_Analysis_Output"
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	primary_folder_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + sample_name + "/Primary_Analysis_Output.metadata.json"
	with open(primary_folder_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_primary_json_template(json_Dict, sample_name, primary_tar_name):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
		{
			"attribute": "object_name",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/" + sample_name + "/Primary_Analysis_Output/" + primary_tar_name
		},
		{
			"attribute": "source_path",
			"value": json_Dict["hpc_dme_path"] + json_Dict["project_id"] + "/" + sample_name + "/Primary_Analysis_Output/"
		},
		{
			"attribute": "platform_name",
			"value": json_Dict["platform_name"]
		}
	]
	metadata_Json = json.dumps(metadata_Dict)
	primary_json_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + sample_name + "/Primary_Analysis_Output/" + primary_tar_name + ".metadata.json"
	with open(primary_json_path, 'w', encoding='utf-8') as f:
		json.dump(metadata_Dict, f, ensure_ascii=False, indent=4)

	return True


def build_project_json_template(json_Dict):
	"""
	"""
	metadata_Dict = {}
	metadata_Dict["metadataEntries"] = [
	{
	"attribute": "access",
	"value": "Controlled Access"
	},
	{
		"attribute": "project_status",
		"value": "Completed"
	},
	{
		"attribute": "project_title",
		"value": json_Dict["project_title"]
	},
	{
		"attribute": "project_poc",
		"value": json_Dict["project_poc"]
	},
	{
		"attribute": "retention_years",
		"value": json_Dict["retention_years"]
	},
	{
		"attribute": "collection_type",
		"value": "Project"
	},
	{
		"attribute": "project_poc_email",
		"value": json_Dict["project_poc_email"]
	},
	{
		"attribute": "project_start_date",
		"value": json_Dict["project_start_date"]
	},
	
	{
		"attribute": "project_id",
		"value": json_Dict["project_id"]
	},
	{
		"attribute": "project_poc_affiliation",
		"value": json_Dict["project_poc_affiliation"]
	}
	]

	metadata_Json = json.dumps(metadata_Dict)
	project_json_file_path = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + ".metadata.json"
	with open(project_json_file_path, 'w', encoding='utf-8') as f:
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


def build_new_structure(json_Dict, scaf_sample_Dict, swarm_file_path):
	"""
	"""
	destination_path = json_Dict["destination_path"]
	if not os.path.exists(destination_path):
		os.makedirs(destination_path,exist_ok = True)
	#+++++++++++++++++++++
	project_directory= json_Dict["destination_path"] + "/" + json_Dict["project_id"]
	if not os.path.exists(project_directory):
		os.makedirs(project_directory,exist_ok = True)
	build_project_json_template(json_Dict)
	#+++++++++++++++++++++
	for each_sample in scaf_sample_Dict:
		##
		if each_sample == "result":
			#analysis
			# ++++++++++++++++++
			analysis_directory = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/Analysis/"
			if not os.path.exists(analysis_directory):
				os.makedirs(analysis_directory,exist_ok = True)
			build_analysis_folder_json_template(json_Dict)
			# ++++++++++++++++++
			for each_analysis in scaf_sample_Dict["result"]:
				##
				source_directory = os.path.dirname(each_analysis)
				analysis_name = each_analysis.split("/")[-1] + ".tar"
				analysis_file_name = analysis_directory + analysis_name
				build_tar_swarm_script(source_directory, each_analysis, analysis_file_name, swarm_file_path)

				#Path(analysis_file_name).touch()
				build_analysis_json_template(json_Dict, analysis_name)

			else:
				##
				pass
			# ++++++++++++++++++
		elif each_sample == "excel_file":
			#excel file
			# ++++++++++++++++++
			analysis_directory = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/Analysis/"
			if not os.path.exists(analysis_directory):
				os.makedirs(analysis_directory,exist_ok = True)
			for each_excel in scaf_sample_Dict["excel_file"]:
				source_directory = os.path.dirname(each_excel)
				excel_name = each_excel.split("/")[-1] + ".tar"
				excel_file_name = analysis_directory + excel_name

				build_tar_swarm_script(source_directory, each_excel, excel_file_name, swarm_file_path)
				#Path(excel_file_name).touch()
				build_analysis_excel_json_template(json_Dict, excel_name)
			# ++++++++++++++++++
		else:
			#primary and fastq
			#+++++++++++++++++++
			sample_directory = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + each_sample
			if not os.path.exists(sample_directory):
				os.makedirs(sample_directory,exist_ok = True)
			build_sample_json_template(json_Dict, each_sample)
			#+++++++++++++++++++
			#+++++++++++++++++++
			fastq_directory = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + each_sample + "/FASTQ"
			if not os.path.exists(fastq_directory):
				os.makedirs(fastq_directory,exist_ok = True)
			build_fastq_folder_json_template(json_Dict, each_sample)
			#+++++++++++++++++++
			#+++++++++++++++++++
			primary_directory = json_Dict["destination_path"] + "/" + json_Dict["project_id"] + "/" + each_sample + "/Primary_Analysis_Output"
			if not os.path.exists(primary_directory):
				os.makedirs(primary_directory,exist_ok = True)
			build_primary_folder_json_template(json_Dict, each_sample)
			#+++++++++++++++++++
			for each_flowcell in scaf_sample_Dict[each_sample]["fastq"]:
				##
				for each_fastq in scaf_sample_Dict[each_sample]["fastq"][each_flowcell]:
					##
					#+++++++++++++++++++
					fastq_file_name = os.path.basename(each_fastq).split(".tar")[0]
					fastq_file_path = fastq_directory + "/" + fastq_file_name + "_FQ_" + each_flowcell + ".tar"
					build_copy_swarm_script(each_fastq, fastq_file_path, swarm_file_path)
					#Path(fastq_file_path).touch()
					fastq_tar_name = fastq_file_name + "_FQ_" + each_flowcell + ".tar"
					build_fastq_json_template(json_Dict, each_sample, fastq_tar_name)
					#+++++++++++++++++++

				else:
					##
					pass
			else:
				##
				pass
			#+++++++++++++++++++
			for each_result_type in scaf_sample_Dict[each_sample]["analysis"]:
				##
				for each_result in scaf_sample_Dict[each_sample]["analysis"][each_result_type]:
					##
					#+++++++++++++++++++++++
					result_file_name = os.path.basename(each_result).split(".tar")[0]
					result_file_path = primary_directory + "/" + result_file_name + "_PA_" + each_result_type + ".tar"
					build_copy_swarm_script(each_result, result_file_path, swarm_file_path)
					#Path(result_file_path).touch()
					result_tar_name = result_file_name + "_PA_" + each_result_type + ".tar"
					build_primary_json_template(json_Dict, each_sample, result_tar_name)
					#+++++++++++++++++++++++
				else:
					pass
			else:
				pass
	else:
		##
		pass
	return True

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
metadata_json = sys.argv[1]

json_Dict = convert_metadata_json_to_dict(metadata_json)



swarm_file_path = json_Dict["swarm_file_path"]
swarm_log_path = json_Dict["swarm_log_dir"]
initiate_swarm_file(swarm_file_path, swarm_log_path)



source_directory = json_Dict["source_directory"]
destination_path = json_Dict["destination_path"]

scaf_sample_Dict = build_scaf_sample_Dict(source_directory)

build_new_structure(json_Dict, scaf_sample_Dict, swarm_file_path)

print("Done!")