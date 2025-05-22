
from django.urls import path
from . import views

urlpatterns = [
    path('sync', views.sync_data, name='sync_data'),
    path('status', views.sync_status, name='sync_status'),
    path('health', views.health_check, name='health_check'),
]
