from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg, Q
import json
import requests
import time
from .models import ApartmentOffer

def geocode_address(address):
    """Get coordinates from address using Yandex Geocoder API"""
    try:
        if not address:
            return None, None

        # Add 'Россия' to the address if not present
        if 'россия' not in address.lower():
            address = f"{address}, Россия"
            
        # Make request to Yandex Geocoder API
        api_key = 'b8c62031-1efc-40b9-bf35-8ee2869c1112'
        url = f"https://geocode-maps.yandex.ru/1.x/?apikey={api_key}&format=json&geocode={requests.utils.quote(address)}"
        
        print(f"Geocoding request for address: {address}")
        
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        
        data = response.json()
        if data['response']['GeoObjectCollection']['featureMember']:
            geo_object = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']
            pos = geo_object['Point']['pos']
            longitude, latitude = map(float, pos.split())
            print(f"Successfully geocoded {address}: {latitude}, {longitude}")
            return latitude, longitude
            
        print(f"No results found for address: {address}")
        return None, None
            
    except Exception as e:
        print(f"Geocoding error for address {address}: {str(e)}")
        return None, None

@csrf_exempt
@require_http_methods(["GET", "POST"])
def test_geocoding(request):
    """Test route for geocoding verification"""
    if request.method == "GET":
        return render(request, 'map/test_geocoding.html')
        
    try:
        data = json.loads(request.body)
        address = data.get('address')
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        
        if not all([address, latitude, longitude]):
            return JsonResponse({
                'error': 'Address and coordinates are required'
            }, status=400)
        
        # Здесь можно добавить код для сохранения координат в базу данных
        # Например, найти объявление по адресу и обновить координаты
        
        return JsonResponse({
            'success': True,
            'address': address,
            'coordinates': {
                'latitude': latitude,
                'longitude': longitude
            }
        })
        
    except Exception as e:
        return JsonResponse({
            'error': str(e)
        }, status=500)

# Список регионов России и их координаты
REGION_COORDINATES = {
    'Москва': {'lat': 55.7558, 'lon': 37.6173, 'zoom': 9},
    'Санкт-Петербург': {'lat': 59.9343, 'lon': 30.3351, 'zoom': 9},
    'Московская область': {'lat': 55.7558, 'lon': 37.6173, 'zoom': 9},
    'Ленинградская область': {'lat': 59.9343, 'lon': 30.3351, 'zoom': 9},
    'Ростовская область': {'lat': 47.2357, 'lon': 39.7015, 'zoom': 9},
    'Краснодарский край': {'lat': 45.0355, 'lon': 38.9753, 'zoom': 9},
    'Свердловская область': {'lat': 56.8389, 'lon': 60.6057, 'zoom': 9},
    'Новосибирская область': {'lat': 55.0084, 'lon': 82.9357, 'zoom': 9},
    'Нижегородская область': {'lat': 56.2965, 'lon': 44.0059, 'zoom': 9},
    'Республика Татарстан': {'lat': 55.7887, 'lon': 49.1221, 'zoom': 9},
    'Самарская область': {'lat': 53.2007, 'lon': 50.1508, 'zoom': 9},
    'Челябинская область': {'lat': 55.1644, 'lon': 61.4368, 'zoom': 9},
    'Республика Башкортостан': {'lat': 54.7348, 'lon': 55.9578, 'zoom': 9},
    'Пермский край': {'lat': 58.0105, 'lon': 56.2502, 'zoom': 9},
    'Воронежская область': {'lat': 51.6720, 'lon': 39.1843, 'zoom': 9},
    'Волгоградская область': {'lat': 48.7194, 'lon': 44.5018, 'zoom': 9},
    'Красноярский край': {'lat': 56.0090, 'lon': 92.8719, 'zoom': 9}, 
    'Саратовская область': {'lat': 51.5406, 'lon': 46.0086, 'zoom': 9},
    'Тюменская область': {'lat': 57.1522, 'lon': 65.5272, 'zoom': 9},
    'Омская область': {'lat': 54.9885, 'lon': 73.3242, 'zoom': 9}
}

# Список регионов России
RUSSIAN_REGIONS = list(REGION_COORDINATES.keys())

@login_required
def index(request):
    """Render the home page with map and filters"""
    return render(request, 'map/home.html', {
        'regions': RUSSIAN_REGIONS,
        'region_coordinates': REGION_COORDINATES
    })


def get_region_from_address(address):
    """Extract region from address"""
    if not address:
        return None
        
    # Нормализуем адрес и регионы для сравнения
    address_lower = address.lower().strip()
    
    # Создаем список вариантов для каждого региона
    for region in RUSSIAN_REGIONS:
        region_lower = region.lower()
        variations = [
            region_lower,
            region_lower.replace(' область', ''),
            region_lower.replace(' край', ''),
            region_lower.replace('республика ', '')
        ]
        
        # Проверяем каждый вариант
        for variation in variations:
            if variation in address_lower:
                return region
                
    # Пытаемся извлечь город из адреса
    common_cities = {
        'москва': 'Москва',
        'санкт-петербург': 'Санкт-Петербург',
        'спб': 'Санкт-Петербург',
        'новосибирск': 'Новосибирская область',
        'екатеринбург': 'Свердловская область',
        'нижний новгород': 'Нижегородская область',
        'казань': 'Республика Татарстан',
        'самара': 'Самарская область',
        'челябинск': 'Челябинская область',
        'уфа': 'Республика Башкортостан',
        'ростов': 'Ростовская область',
        'краснодар': 'Краснодарский край',
        'воронеж': 'Воронежская область',
        'волгоград': 'Волгоградская область',
        'пермь': 'Пермский край',
        'тюмень': 'Тюменская область',
        'саратов': 'Саратовская область',
        'красноярск': 'Красноярский край',
        'омск': 'Омская область'
    }
    
    for city, region in common_cities.items():
        if city in address_lower:
            return region
            
    return None

def extract_price(price_str):
    """
    Извлекает числовое значение цены из строки.
    Примеры входных строк:
    - "20000.0 руб./ За месяц, Залог - 20000 руб."
    - "20,000 руб"
    - "20000"
    """
    try:
        # Преобразуем в строку и уберем пробелы в начале и конце
        price_str = str(price_str).strip()
        
        # Если строка пустая, возвращаем 0
        if not price_str:
            return 0
            
        # Разбиваем по пробелу и берем первую часть
        first_part = price_str.split()[0]
        
        # Убираем все пробелы и заменяем запятые на точки
        clean_number = first_part.replace(' ', '').replace(',', '.')
        
        # Находим первое число в строке (до первого нечислового символа)
        import re
        number_match = re.match(r'^(\d+\.?\d*)', clean_number)
        if number_match:
            return float(number_match.group(1))
            
        return 0
    except Exception as e:
        print(f"Error extracting price from '{price_str}': {str(e)}")
        return 0

@csrf_exempt  # Only for development, remove in production
@require_http_methods(["POST"])
@login_required
def get_properties(request):
    """Get filtered properties"""
    try:
        filters = json.loads(request.body)
        
        # Start with all active properties
        queryset = ApartmentOffer.objects.filter(
            actual_flg=1,
            delete_flg=0
        )

        # Apply filters
        if filters.get('region'):
            queryset = queryset.filter(address__icontains=filters['region'])
        
        if filters.get('minPrice'):
            try:
                min_price = float(filters['minPrice'])
                queryset = queryset.filter(price__regex=r'^\d+').extra(
                    where=[f"CAST(REGEXP_REPLACE(SPLIT_PART(price, ' ', 1), '[^0-9.]', '', 'g') AS DECIMAL) >= {min_price}"]
                )
            except ValueError:
                pass
            
        if filters.get('maxPrice'):
            try:
                max_price = float(filters['maxPrice'])
                queryset = queryset.filter(price__regex=r'^\d+').extra(
                    where=[f"CAST(REGEXP_REPLACE(SPLIT_PART(price, ' ', 1), '[^0-9.]', '', 'g') AS DECIMAL) <= {max_price}"]
                )
            except ValueError:
                pass
            
        if filters.get('rooms'):
            room_filters = Q()
            for room in filters['rooms']:
                room_filters |= Q(amount_rooms__icontains=str(room))
            queryset = queryset.filter(room_filters)

        # Convert queryset to list of dictionaries
        properties = []
        
        for prop in queryset:
            try:
                # Extract price
                price_value = extract_price(prop.price)
                
                # Get coordinates
                latitude = prop.latitude
                longitude = prop.longitude
                
                # If no coordinates, try to geocode
                if latitude is None or longitude is None:
                    latitude, longitude = geocode_address(prop.address)
                    
                    # Save coordinates if geocoding was successful
                    if latitude is not None and longitude is not None:
                        prop.latitude = latitude
                        prop.longitude = longitude
                        prop.save(update_fields=['latitude', 'longitude'])
                
                # Only add property if we have coordinates
                if latitude is not None and longitude is not None:
                    properties.append({
                        'id': prop.id_offer,
                        'title': f"{prop.amount_rooms}-комнатная квартира",
                        'latitude': latitude,
                        'longitude': longitude,
                        'price': price_value,
                        'rooms': prop.amount_rooms,
                        'region': prop.address,
                        'square': prop.square,
                        'description': prop.description
                    })
                else:
                    print(f"Skipping property {prop.id_offer} - no coordinates available")
                    
            except Exception as e:
                print(f"Error processing property {prop.id_offer}: {str(e)}")
                continue

        # Calculate summary statistics
        total_price = sum(prop['price'] for prop in properties)
        avg_price = total_price / len(properties) if properties else 0

        response_data = {
            'properties': properties,
            'summary': {
                'count': len(properties),
                'averagePrice': round(avg_price, 2)
            }
        }

        return JsonResponse(response_data)
        
    except Exception as e:
        print(f"Error in get_properties: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def bulk_geocoding(request):
    """Page for bulk geocoding addresses from database"""
    if request.method == "POST":
        # Get offers without coordinates
        offers = ApartmentOffer.objects.filter(
            Q(latitude__isnull=True) | Q(longitude__isnull=True),
            actual_flg=1,
            delete_flg=0
        )
        
        results = []
        for offer in offers:
            if not offer.address:
                continue
                
            lat, lon = geocode_address(offer.address)
            if lat and lon:
                offer.latitude = lat
                offer.longitude = lon
                offer.save(update_fields=['latitude', 'longitude'])
                
                results.append({
                    'id': offer.id_offer,
                    'address': offer.address,
                    'coordinates': f"{lat}, {lon}",
                    'status': 'success'
                })
            else:
                results.append({
                    'id': offer.id_offer,
                    'address': offer.address,
                    'coordinates': None,
                    'status': 'failed'
                })
        
        return JsonResponse({'results': results})
    
    # GET request - show the page
    offers_without_coords = ApartmentOffer.objects.filter(
        Q(latitude__isnull=True) | Q(longitude__isnull=True),
        actual_flg=1,
        delete_flg=0
    ).count()
    
    return render(request, 'map/bulk_geocoding.html', {
        'offers_without_coords': offers_without_coords
    })

@csrf_exempt
@require_http_methods(["POST"])
@login_required
def save_coordinates(request):
    """Save coordinates for an address"""
    try:
        data = json.loads(request.body)
        address = data.get('address')
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        
        if not all([address, latitude, longitude]):
            return JsonResponse({
                'error': 'Address and coordinates are required'
            }, status=400)
            
        # Найдем все объявления с таким адресом
        offers = ApartmentOffer.objects.filter(
            address__icontains=address,
            actual_flg=1,
            delete_flg=0
        )
        
        updated_count = 0
        for offer in offers:
            offer.latitude = latitude
            offer.longitude = longitude
            offer.save(update_fields=['latitude', 'longitude'])
            updated_count += 1
            
        return JsonResponse({
            'success': True,
            'message': f'Updated coordinates for {updated_count} offers',
            'updated_count': updated_count
        })
        
    except Exception as e:
        return JsonResponse({
            'error': str(e)
        }, status=500)