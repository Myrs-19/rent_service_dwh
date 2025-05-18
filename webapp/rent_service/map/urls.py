from django.urls import path

from . import views

app_name = 'map'

urlpatterns = [
    path("", views.index, name="index"),
    path('properties/', views.get_properties, name='get_properties'),
    path('test-geocoding/', views.test_geocoding, name='test_geocoding'),
    path('bulk-geocoding/', views.bulk_geocoding, name='bulk_geocoding'),
    path('api/save-coordinates/', views.save_coordinates, name='save_coordinates'),
]
