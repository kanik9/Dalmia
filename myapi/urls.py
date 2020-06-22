from django.urls import path, include
from myapi.views import snippet_list

urlpatterns = [
    path('dalmia_file_upload/', snippet_list),
]
