# functional libraries used
from datetime import datetime
import collections  # using namedtuple to return multiple results from a function

# technical distribution libraries used
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


startTime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
print('run start time: ' + startTime)


def load_user_settings():
    run_parameters = {
        # sample file is assumed not having header row, headers are stored in run paramters of the cashflow script
        "sample_file_directory": "C:/IRR_CF_Data/input data/Input_Sample_Data_3000.csv",
        "replication_volume": 100,
        "volume_file_output_cloud_directory": "gs://sweet-gooseberry-upload/IRR_CF_Engine/input/",
        "volume_file_output_local_directory": "C:/IRR_CF_Data/input data/",
        "cloud_job_name_prefix": "sweetgooseberry-145819-irr-generate-test-data",
        # options NONE, THROUGHPUT_BASED
        "autoscaler_setting": "NONE",
        # using us-central1 region, max resourse quota is 24-25 as of 201707
        "num_workers_setting": "25",
        # runner options: DirectRunner, DataflowRunner
        "runner": 'DirectRunner',
    }
    return run_parameters


# optional feature to cater for cmd and manual trigger
# TD more efficient way of defining default paramters when running via cmd
'''
if len(sys.argv) == 3:
    replications = sys.argv[1]
    sample_file_input = sys.argv[2]
elif len(sys.argv) == 2:
    replications = sys.argv[1]
    sample_file_input = default_sample_file_input
else:
    replications = default_replication_volume
    sample_file_input = default_sample_file_input
'''


def read_sample_data(run_parameters):
    sample_data_file = open(run_parameters['sample_file_directory'], 'r')
    # readlines() return a list of strings (includeing the '/n' at the end for new row)
    sample_data_list = sample_data_file.readlines()
    sample_volume = len(sample_data_list)
    # using namedtuple allows for both results[0] and results.sample_data references
    results = collections.namedtuple('sample', 'sample_data_list,sample_volume')(sample_data_list, sample_volume)
    return results


def replicate_data(sample_data, replication_volume):
    volume_data = ''
    for i in range(replication_volume):
        volume_data += sample_data
    volume_data = volume_data.strip()
    return volume_data


def generate_data():

    # create run parameters object with all user settings as attributes
    run_parameters = load_user_settings()

    # read input sample data
    sample_data_namedtuple = read_sample_data(run_parameters)
    sample_data_list = sample_data_namedtuple.sample_data_list

    # generate unique key for labelling
    sample_volume = sample_data_namedtuple.sample_volume
    replication_volume = run_parameters['replication_volume']
    unique_key = str(sample_volume) + "x" + str(replication_volume) + "-" + startTime

    # set output volume data directory
    volume_file_output_cloud_directory = run_parameters['volume_file_output_cloud_directory']
    volume_file_output_local_directory = run_parameters['volume_file_output_local_directory']
    runner = run_parameters['runner']
    if runner == 'DataflowRunner':
        volume_file_output = volume_file_output_cloud_directory + unique_key + "/Input_Volume_Data_" + unique_key + ".csv"
    else:
        volume_file_output = volume_file_output_local_directory + unique_key + "/data.csv"

    # define pipleline settings
    parser = argparse.ArgumentParser()
    parser.add_argument('--outputTestData',
                        dest='outputTestData',
                        # for outputting the results.
                        # output paths for dataflow / direct runner are configured above
                        default=volume_file_output,
                        help='Output file to write test volume data to.')
    known_args, pipeline_args = parser.parse_known_args(None)
    pipeline_args.extend([
        '--runner={0}'.format(runner),
        '--project=sweetgooseberry-145819',
        '--staging_location=gs://sweet-gooseberry-upload/IRR_CF_Engine/staging',
        '--temp_location=gs://sweet-gooseberry-upload/IRR_CF_Engine/temp/',
        '--job_name={0}'.format(run_parameters['cloud_job_name_prefix'] + "-" + unique_key),
        '--autoscaling_algorithm={0}'.format(run_parameters['autoscaler_setting']),
        '--num_workers={0}'.format(run_parameters['num_workers_setting']),
    ])
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    (p
     | 'Create transaction level data' >> beam.Create(sample_data_list)
     | 'Convert into objects' >> beam.Map(lambda sample_data: replicate_data(sample_data, replication_volume))
     | 'Save results' >> beam.io.WriteToText(known_args.outputTestData))

    # when running locally wait until pipepline finishes to print the end time message
    # when running on cloud, stated cloud run is triggered and can be monitored online
    if runner == 'DataflowRunner':
        p.run()
        print('run started on Google Cloud, please check via Dataflow Monitor')
    else:
        p.run().wait_until_finish()
        end_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        print('run end time: ' + end_time)


if __name__ == "__main__":
    generate_data()

''' 
performance
- 3 x 1mil, autoscaler (1 worker): 5.5 min, 300 MB x 3 files
- 3 x 10k, 3 workers: 4.5 min, 30 MB x 3 files
- 3 x 100k, 3 workers: 4.5 min, 30 MB x 3 files
- 3 x 100, 3 workers: 4.5 min, 30 KB x 3 files

- 30 x 1mil, 12 workers: 5.5 min, 300 MB x 30 files
>> (almost) linear nature of cloud computing!
>> preference over a more distributable base
>> file output pattern, each input record is producing a separate output file
- 30 x 100, 20 workers: 4.5 min, 30KB x 30 files = 900KB
- 30 x 1k, 5 workers: 4 min, 15 files = 9MB
- 30 x 10k, 5 workers: 4.5 min, 15 files = 90MB
- 30 x 100k, 20 workers: 4.5 min, 30 files 900MB
- 30 x 1mil, 22/25 workers: 10.5 min, 30 files 8.8GB

- 30 x 10mil (failed), 22/25 workers: 24 min,  files GB
- 30 x 5mil (failed), 22/25 workers: 16 min,  files GB
>> PCollection is held in memory so pipeline can be too large for single worker
>> max per worker load is 1mil in memory for this particular pipeline

- 3000 x 100k (300mil), 22/25 workers: 6 min, 75 files 87GB


'''