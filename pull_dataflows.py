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

tnso = sdmx.Request('TNSO')

allDataFlows = tnso.get(resource_type='dataflow')

# for df in allDataFlows.dataflow:
#     try:
#        print(tnso.datastructure(df))
#     except req.HTTPError as err:
#        print(err)

# t = tnso.datastructure('DSD_01IND_CEN').to_pandas()
# i = 0
# while (i < len(t.codelist)):
#     print(t.codelist['CL_AREA'])
#     i += 1

t = tnso.codelist('CL_AREA').to_pandas()
print(t)

# resp = tnso.data('DF_11AG_AQUA', params={'startPeriod': '2560'},).to_pandas()
# print(resp)