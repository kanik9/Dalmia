from django.shortcuts import render

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.core.files.storage import FileSystemStorage
import os
from threading import Thread
import json
from myapi.dalmia_data_preprocessing_full_load import main
# Create your views here.


@api_view(['GET', 'POST'])
def snippet_list(request):
    if request.method == 'GET':
        return JsonResponse({"message": "hello world"})
    if request.method == "POST":
        try:
            for i in os.listdir("files/"):
                os.remove("files/" + i)
            f = open('config.json')
            config = json.load(f)
            f.close()
            
            switch_case = ['VC Input', 'VC Model', 'NCR Input', 'FC Model']
            for keys in switch_case:
                if keys in request.data.keys():
                    file = request.data[keys] 
                    file_store = FileSystemStorage()
                    file_store.save("files/"+file.name, file)  
                    file_name = 'files/'+file.name
                    config[keys] = 'files/'+file.name
                    with open('config.json', 'w') as f:
                        f.write(json.dumps(config))
            plant_name = request.data['plant_name']
            thread_script = Thread(target=main, args=(plant_name, file_name))
            thread_script.start()
        except Exception as e:
            print(e)
            return JsonResponse({"message": "error encounterd "+str(e)})
        return JsonResponse({"message": "we got the files"})
