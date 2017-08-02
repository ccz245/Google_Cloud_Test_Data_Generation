import csv
from datetime import datetime
import sys

# version live run
# replications = 1000000
# version for cmd
if len(sys.argv) > 1:
    replications = sys.argv[1]
else:
    replications = 10

startTime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
print('run start time: ' + startTime)

# the sample file has no header row
sample_file = "C:/Users/charl/Google Drive/10. Coding/9. IRR CF Engine Python/Google Cloud/input data/Input_Sample_Data.csv"
volume_file = "C:/Users/charl/Google Drive/10. Coding/9. IRR CF Engine Python/Google Cloud/input data/Input_Volume_Data_" + str(replications) + "-" + startTime + ".csv"

sample_data = open(sample_file, 'r')
reader = csv.reader(sample_data)

# a list of lists, each individual list ia a row of data, each field is an element of that list
data_list = list(reader)

# create volume data file
volume_data = open(volume_file, 'a+')

# configure writer to write standard csv file
writer = csv.writer(volume_data, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

# replicate data into volume data file
for i in range(len(data_list)):
    deal_data = data_list[i]
    for j in range(replications):
        writer.writerow(deal_data)

endTime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
print('run end time: ' + endTime)