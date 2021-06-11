####################################################################
#
# process the csv file into Elasticsearch json documents
#
####################################################################

import os
import csv
import json
from datetime import datetime

stationsFile = 'ghcnd-stations.txt'
countriesFile = 'ghcnd-countries.txt'
statesFile = 'ghcnd-states.txt'

weatherDataFiles = ['2014-sorted.csv', '2015-sorted.csv', '2016-sorted.csv']
indexPrefix = 'weather-data'
docType = 'summary'

def loadStatesFile(statesFile):
    statesMap = {}
    with open(statesFile, 'r') as file:
        csvreader = csv.reader(file, delimiter=' ', quotechar='"')
        for row in csvreader:
            statesMap[row[0].strip()] = row[1].strip()
    return statesMap

def loadCountriesFile(countriesFile):
    countriesMap = {}
    with open(countriesFile, 'r') as file:
        csvreader = csv.reader(file, delimiter=' ', quotechar='"')
        for row in csvreader:
            countriesMap[row[0].strip()] = row[1].strip()
    return countriesMap

def loadStationsFile(stationsFile, statesFile, countriesFile):
    statesMap = loadStatesFile(statesFile)
    countriesMap = loadCountriesFile(countriesFile)
    stationsMap = {}
    with open(stationsFile, 'r') as file:
        for row in file:
            try:
                station = {}
                station['id'] = row[0:11].strip()
                countryCode = row[0:2].strip()
                if len(countryCode) > 0:
                    station['country_code'] = countryCode
                    station['country'] = countriesMap[countryCode]
                station['location'] = {
                    'lat': float(row[12:20].strip()),
                    'lon': float(row[21:30].strip())
                }
                station['elevation'] = float(row[31:37].strip())
                if countryCode == 'US':
                    stateCode = row[38:40].strip()
                    if len(stateCode) > 0:
                        station['state_code'] = stateCode
                        station['state'] = statesMap[stateCode]
                station['name'] = row[41:71].strip()
                gsn_flag = row[72:75].strip()
                if len(gsn_flag) > 0:
                    station['gsn_flag'] = gsn_flag
                hcn_crn_flag = row[76:78].strip()
                if len(hcn_crn_flag) > 0:
                    station['hcn_crn_flag'] = hcn_crn_flag
                wmo_id = row[80:85].strip()
                if len(wmo_id) > 0:
                    station['wmo_id'] = wmo_id
                stationsMap[station['id']] = station
            except:
                print(row)
                raise e
    return stationsMap

def processWeatherDoc(currentStationDoc):
    if 'TMAX' in currentStationDoc:
        currentStationDoc['TMAX'] = float(currentStationDoc['TMAX']) / 10.0
    if 'TMIN' in currentStationDoc:
        currentStationDoc['TMIN'] = float(currentStationDoc['TMIN']) / 10.0
    if 'PRCP' in currentStationDoc:
        currentStationDoc['PRCP'] = float(currentStationDoc['PRCP']) / 10.0
    if 'AWND' in currentStationDoc:
        currentStationDoc['AWND'] = float(currentStationDoc['AWND']) / 10.0
    if 'EVAP' in currentStationDoc:
        currentStationDoc['EVAP'] = float(currentStationDoc['EVAP']) / 10.0
    if 'MDEV' in currentStationDoc:
        currentStationDoc['MDEV'] = float(currentStationDoc['MDEV']) / 10.0
    if 'MDPR' in currentStationDoc:
        currentStationDoc['MDPR'] = float(currentStationDoc['MDPR']) / 10.0
    if 'MDTN' in currentStationDoc:
        currentStationDoc['MDTN'] = float(currentStationDoc['MDTN']) / 10.0
    if 'MDTX' in currentStationDoc:
        currentStationDoc['MDTX'] = float(currentStationDoc['MDTX']) / 10.0
    if 'MNPN' in currentStationDoc:
        currentStationDoc['MNPN'] = float(currentStationDoc['MNPN']) / 10.0
    if 'MXPN' in currentStationDoc:
        currentStationDoc['MXPN'] = float(currentStationDoc['MXPN']) / 10.0
    if 'TAVG' in currentStationDoc:
        currentStationDoc['TAVG'] = float(currentStationDoc['TAVG']) / 10.0
    if 'THIC' in currentStationDoc:
        currentStationDoc['THIC'] = float(currentStationDoc['THIC']) / 10.0
    if 'TOBS' in currentStationDoc:
        currentStationDoc['TOBS'] = float(currentStationDoc['TOBS']) / 10.0
    if 'WESD' in currentStationDoc:
        currentStationDoc['WESD'] = float(currentStationDoc['WESD']) / 10.0
    if 'WESF' in currentStationDoc:
        currentStationDoc['WESF'] = float(currentStationDoc['WESF']) / 10.0
    if 'WSF1' in currentStationDoc:
        currentStationDoc['WSF1'] = float(currentStationDoc['WSF1']) / 10.0
    if 'WSF2' in currentStationDoc:
        currentStationDoc['WSF2'] = float(currentStationDoc['WSF2']) / 10.0
    if 'WSF5' in currentStationDoc:
        currentStationDoc['WSF5'] = float(currentStationDoc['WSF5']) / 10.0
    if 'WSFG' in currentStationDoc:
        currentStationDoc['WSFG'] = float(currentStationDoc['WSFG']) / 10.0
    if 'WSFI' in currentStationDoc:
        currentStationDoc['WSFI'] = float(currentStationDoc['WSFI']) / 10.0
    if 'WSFM' in currentStationDoc:
        currentStationDoc['WSFM'] = float(currentStationDoc['WSFM']) / 10.0

    if 'TMIN' in currentStationDoc and 'TMAX' in currentStationDoc:
        if currentStationDoc['TMIN'] > currentStationDoc['TMAX']:
            tmp = currentStationDoc['TMIN']
            currentStationDoc['TMIN'] = currentStationDoc['TMAX']
            currentStationDoc['TMAX'] = tmp
        currentStationDoc['TRANGE'] = {
            "gte" : currentStationDoc['TMIN'],
            "lte" : currentStationDoc['TMAX']
        }
    if 'MDTN' in currentStationDoc and 'MDTX' in currentStationDoc:
        if currentStationDoc['MDTN'] > currentStationDoc['MDTX']:
            tmp = currentStationDoc['MDTN']
            currentStationDoc['MDTN'] = currentStationDoc['MDTX']
            currentStationDoc['MDTX'] = tmp
        currentStationDoc['MDTRANGE'] = {
            "gte" : currentStationDoc['MDTN'],
            "lte" : currentStationDoc['MDTX']
        }

    indexDoc = {
        '_op_type': 'create',
        '_index': indexPrefix + '-' + str(currentStationDoc['date'].year),
        '_type': docType,
        '_id': currentStationDoc['date'].strftime('%Y-%m-%d') + '-' + currentStationDoc['station']['id'],
        '_source': currentStationDoc
    }
    return indexDoc

def processWeatherFile(weatherDataFile, stationsMap):
    with open(weatherDataFile, 'r') as file:
        csvreader = csv.reader(file, delimiter=',', quotechar='"')
        currentStationDoc = None
        stationDocsProcessed = 0
        for row in csvreader:
            station = stationsMap[row[0]]
            date = datetime.strptime(row[1], '%Y%m%d')
            elementType = row[2]
            elementValue = row[3]
            if currentStationDoc == None:
                currentStationDoc = {
                    'station': station,
                    'date': date,
                    elementType: elementValue
                }
            elif currentStationDoc['station'] != station or currentStationDoc['date'] != date:
                yield processWeatherDoc(currentStationDoc)
                stationDocsProcessed = stationDocsProcessed + 1
                currentStationDoc = {
                    'station': station,
                    'date': date,
                    elementType: elementValue
                }
            else:
                currentStationDoc[elementType] = elementValue

stationsMap = loadStationsFile(stationsFile, statesFile, countriesFile)
outFile = 'documents.json'
with open(outFile, 'w+') as file:
    count = 0
    for weatherDataFile in weatherDataFiles:
        for doc in processWeatherFile(weatherDataFile, stationsMap):
            doc['_source']['date'] = doc['_source']['date'].isoformat()
            file.write(json.dumps(doc['_source']))
            file.write('\n')
            count = count + 1
print('Wrote ' + str(count) + ' entries')