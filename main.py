import pandasdmx as sdmx
import requests as req
import re
import pandas

sdmx.add_source({
  "id": "TNSO",
  "documentation": "https://ns1-stathub.nso.go.th",
  "url": "https://ns1-stathub.nso.go.th/rest",
  "name": "Thailand National Statistics Office",
  "data_content_type": "XML"
})

def extractCodeListFromDimension(dimension):
    if not dimension['id'] == 'TIME_PERIOD':
        s = re.match(r'.*:(.*)\(\d\.\d\)$', dimension['representation']['representation'])
        return s.group(1)
    else:
        return 'TIME_PERIOD'

def fetchDimensionCodeList(datastructureId):
    response = req.get('https://sdmx.nso.go.th/FusionMetadataRegistry/sdmx/v2/structure/datastructure/TNSO/' + datastructureId + '/latest/?format=fusion-json')
    if response.ok:
        dimensions = response.json()['DataStructure'][0]['dimensionList']['dimensions']
        return list(map(extractCodeListFromDimension, dimensions))
    else:
        return []

csvConceptSchemes = []
csvDatastructures = []
csvDimensions = []
csvHasAllDimensions = []
csvLackDimensions = []

tnso = sdmx.Request('TNSO')

categorySchemes = tnso.get(resource_type='categoryscheme')

for dsdId in categorySchemes.structure:
    try:
        dsd = tnso.get(resource_type='datastructure', resource_id=dsdId)
        csvConceptSchemes.append(list(dsd.concept_scheme)[0])
        csvDatastructures.append(list(dsd.structure)[0])

        fusionCodeList = fetchDimensionCodeList(dsdId)
        csvDimensions.append(fusionCodeList)

        if fusionCodeList:
            lackDimensions = []
            for cl in fusionCodeList:
                if cl not in dsd.codelist and not cl == 'TIME_PERIOD':

                    if not lackDimensions:
                        csvHasAllDimensions.append(False)
                    lackDimensions.append(cl)

            if not lackDimensions:
                csvHasAllDimensions.append(True)

            csvLackDimensions.append(lackDimensions)

        else:
            csvHasAllDimensions.append('Unable to check')
            csvLackDimensions.append('Unable to check')

    except req.HTTPError as err:
        csvConceptSchemes.append('-')
        csvDatastructures.append('Datastructure ' + dsdId + ': ' + err.response.text)
        csvDimensions.append('-')
        csvHasAllDimensions.append('Skipped')
        csvLackDimensions.append('-')


csvDf = pandas.DataFrame({'conceptschemes': csvConceptSchemes,
                          'datastructures': csvDatastructures,
                          'dimensions': csvDimensions,
                          'hasAllDimensions': csvHasAllDimensions,
                          'lackDimensions': csvLackDimensions})
csvDf.index += 1

csvDf.to_csv('~/Desktop/dim.csv', columns=['conceptschemes',
                                           'datastructures',
                                           'dimensions',
                                           'hasAllDimensions',
                                           'lackDimensions'])