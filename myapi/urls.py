from django.urls import path
from django.conf import settings
#from myapi.views import ThankView
from django.conf.urls.static import static
from myapi.view_api import snippet_list_fc
from myapi.view_api_vc import snippet_list_vc
from myapi.view_api_ncr import snippet_list_ncr
from django.views.generic import TemplateView


urlpatterns = [
    path('api_ncr/', snippet_list_ncr),
    path('api_vc/', snippet_list_vc),
    path('api_fc/', snippet_list_fc),
    path("", TemplateView.as_view(template_name='base.html')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, root=settings.STATICFILES_DIRS)


