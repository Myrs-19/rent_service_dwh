from django.db import models


class ApartmentOffer(models.Model):
    source_system_dk = models.TextField(verbose_name="Source System DK", blank=True, null=True)
    apartment_offer_rk = models.TextField(verbose_name="Apartment Offer RK")
    valid_from = models.DateTimeField(verbose_name="Valid From", blank=True, null=True)
    hashdiff_key = models.TextField(verbose_name="HashDiff Key", blank=True, null=True)
    actual_flg = models.SmallIntegerField(verbose_name="Actual Flag", blank=True, null=True)
    delete_flg = models.SmallIntegerField(verbose_name="Delete Flag", blank=True, null=True)
    id_offer = models.IntegerField(verbose_name="Offer ID", blank=True, null=True)
    amount_rooms = models.TextField(verbose_name="Amount of Rooms", blank=True, null=True)
    offer_type = models.TextField(verbose_name="Offer Type", blank=True, null=True)
    address = models.TextField(verbose_name="Address")
    square = models.TextField(verbose_name="Square", blank=True, null=True)
    house_address = models.TextField(verbose_name="House Address", blank=True, null=True)
    parking_space = models.TextField(verbose_name="Parking Space", blank=True, null=True)
    price = models.TextField(verbose_name="Price")
    phones = models.TextField(verbose_name="Phones", blank=True, null=True)
    description = models.TextField(verbose_name="Description", blank=True, null=True)
    repair = models.TextField(verbose_name="Repair", blank=True, null=True)
    square_rooms = models.TextField(verbose_name="Square Rooms", blank=True, null=True)
    balcony = models.TextField(verbose_name="Balcony", blank=True, null=True)
    windows_oriention = models.TextField(verbose_name="Windows Orientation", blank=True, null=True)
    bathroom = models.TextField(verbose_name="Bathroom", blank=True, null=True)
    is_possible_with_kids_animals = models.TextField(verbose_name="Kids & Animals Allowed", blank=True, null=True)
    additional_description = models.TextField(verbose_name="Additional Description", blank=True, null=True)
    residential_complex_title = models.TextField(verbose_name="Residential Complex Title", blank=True, null=True)
    ceiling_height = models.TextField(verbose_name="Ceiling Height", blank=True, null=True)
    lift = models.TextField(verbose_name="Lift", blank=True, null=True)
    garbage_chute = models.TextField(verbose_name="Garbage Chute", blank=True, null=True)
    link_to_offer = models.TextField(verbose_name="Link to Offer", blank=True, null=True)
    latitude = models.FloatField(verbose_name="Latitude", blank=True, null=True)
    longitude = models.FloatField(verbose_name="Longitude", blank=True, null=True)

    class Meta:
        db_table = 'apartment_offer'  # имя таблицы в БД (если нужно явно указать)
        verbose_name = 'Apartment Offer'
        verbose_name_plural = 'Apartment Offers'

    def __str__(self):
        return f"Offer #{self.id_offer} - {self.address}"