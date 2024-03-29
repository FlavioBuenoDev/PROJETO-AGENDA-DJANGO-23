from django.contrib import admin
from contact import models


#  Register your models here.
# Usando o Decorator para registrar model admin
@admin.register(models.Contact)
class ContactAdmin(admin.ModelAdmin):
    list_display = 'id', 'first_name', 'last_name', 'phone',
    ordering = '-id',
    # list_filter = 'created_date',
    search_fields = 'id', 'first_name', 'last_name',
    list_per_page = 10
    list_max_show_all = 200
    list_editable = 'first_name', 'last_name',
    list_display_links = 'id', 'phone',


@admin.register(models.Category)
class Category(admin.ModelAdmin):
    list_display = 'name',
    ordering = '-id',
   