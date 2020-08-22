from rest_framework.decorators import api_view
from django.http.response import JsonResponse
from django.core.files.storage import FileSystemStorage
import os
from threading import Thread

from myapi.main import main


# Create your views here.


@api_view(['GET', 'POST'])
def snippet_list_fc(request):
    if request.method == 'GET':
        return JsonResponse({"message": "hello world"})


    if request.method == "POST":

        try:
            for i in os.listdir("media/"):
                os.remove("media/" + i)

            keys = 'fc_model'
            request_file = request.FILES[keys] if keys in request.FILES.keys() else None
            if request_file:
                fs = FileSystemStorage()
                file = fs.save(request_file.name, request_file)
                file_name = fs.url(file)
                print("File Name : ", file_name)

                plant_id = request.data['id']
                print(plant_id)
                thread_script = Thread(target=main, args=(plant_id, keys))
                thread_script.start()
                thread_script.join()
                print("END")

            else:
                print("Message : " + 'Key is different' + str(request.FILES.keys()))



        except Exception as e:
            print(e)
            return JsonResponse({"message": "error encountered " + str(e)})
        return JsonResponse({"message": "we got the files"})
