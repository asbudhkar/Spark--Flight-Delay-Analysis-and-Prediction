# Code to get weather forecasts given the area zip code and store in the file

#!/usr/bin/python

# Import required libraries
import sys
import json
import os
from noaa_sdk import noaa

def main():
    
    # Get command line arguments
    zip = int(sys.argv[1])
    date = sys.argv[2]
    dist = int(sys.argv[3])
    airport = (sys.argv[4])

    #  Get instance of NOAA 
    n = noaa.NOAA()

    # Use get_forecasts method to get all weather forecasts for 5 days for given zip code area
    res = n.get_forecasts(zip, 'US', True)

    # Select the forcast matching the date 
    for i in res:
        if i['startTime'].split('T')[0]==date:
            break

    # Snow and Rain intially zero        
    snow=0
    prec=0

    # Open file to write inputs
    file1 = open("myfile.txt","w")

    # get wind speed
    wind = i['windSpeed'].split(" ")[0]

    # get temperature 
    temp = i['temperature']
 
    # Get month
    mon= int(date.split("-")[1])

    # Create arrays of average precipitation and snow in inch
    pjfk = [3.15,2.6,3.78,3.86,3.94,3.86,4.09,3.66,3.5,3.62,3.31,3.39]
    psfo = [4.21,4.15,3.4,1.25,0.54,0.13,0.04,0.09,0.28,1.19,3.31,3.18]
    pord = [1.73,1.77,2.52,3.39,3.66,3.46,3.7,4.88,3.23,3.15,2.24]
    pdfw = [1.9,2.37,3.06,3.20,5.15,3.23,2.12,2.03,2.42,4.11,2.57,2.57]
    plax = [2.71,3.25,1.85,0.70,0.22,0.08,0.03,0.05,0.21,0.56,1.11,2.05]
    jfk = [6,8,4,1,0,0,0,0,0,0,0,5]
    sfo = [0,0,0,0,0,0,0,0,0,0,0,0]
    ord = [11,9,6,1,0,0,0,0,0,0,1,9]
    dfw = [0.3,0.9,0.2,0,0,0,0,0,0,0,0,0.3] 
    lax = [0,0,0,0,0,0,0,0,0,0,0,0]
    
    forecast = i['shortForecast']
    
    # If forecast is rain  or snow use the average values for the month 
    if ("rain" in forecast.lower()):
         if (airport == "JFK"):
         	prec=pjfk[mon-1]
         elif (airport == "SFO"):
           prec=psfo[mon-1]
         elif (airport == "ORD"):
           prec=pord[mon-1]
         elif (airport == "LAX"):
           prec=plax[mon-1]
         elif (airport == "DFW"):
           prec=pdfw[mon-1]  
    if ("snow" in forecast.lower()):
         if (airport == "JFK"):
         	snow=jfk[mon-1]
         elif (airport == "SFO"):
           snow=sfo[mon-1]
         elif (airport == "ORD"):
           snow=ord[mon-1]
         elif (airport == "LAX"):
           snow=lax[mon-1]
         elif (airport == "DFW"):
           snow=dfw[mon-1]  

    # Save all features in a file
    file1.write(str(20)+","+str(10)+","+str(wind)+","+str(temp)+","+str(temp)+","+str(prec)+","+str(snow)+","+str(dist))  

    file1.close()

    # Copy the file to HPC Cluster for further analysis
    os.system("scp myfile.txt asb862@dumbo.hpc.nyu.edu:")

if __name__ == "__main__":
    main()