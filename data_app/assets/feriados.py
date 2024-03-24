from dagster import asset, AssetIn

from bs4 import BeautifulSoup

import csv
import re
import requests
import urllib.parse
import time

import json
from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse


# AssetKey = if not specified, it takes the name of decorated function ('lista_tuplas_por_pais_desde_wikipedia')
# Op = the lista_tuplas_por_pais_desde_wikipedia function
# Upstream assets = empty (Optional)
@asset(group_name="lagos")
def filtrar_divs_nombres_paises_areas():
    pagina = obtener_informacion_pagina()
    divs_nombres_pais_area = obtener_nombres_paises_areas_pag(pagina)
    return filtrar_nombres_paises_areas(divs_nombres_pais_area)   


@asset(group_name="lagos")
def extraer_datos_lagos(filtrar_divs_nombres_paises_areas):
    return [extraer_datos(div_lago) for div_lago in filtrar_divs_nombres_paises_areas]


@asset(group_name="lagos")
def guardar_datos_lagos_en_archivo_csv(extraer_datos_lagos) -> None:
    with open("datos_lagos.csv", "w", encoding="latin1", newline="") as file_writer:
        writer = csv.DictWriter(file_writer, fieldnames=extraer_datos_lagos[0].keys())
        writer.writeheader()
        writer.writerows(extraer_datos_lagos)


# Web scraping desde wikipedia
def obtener_informacion_pagina():
    url = "https://www.howlanders.com/blog/latam/lagos-mas-grandes-de-sudamerica/amp/"
    respuesta = requests.get(url)
    pagina    = BeautifulSoup(respuesta.text, "html.parser" )
    return pagina


def obtener_nombres_paises_areas_pag(pagina):
    nombres_y_paises_lagos_pag = pagina.find_all(name='h2')
    areas_lagos_pag = pagina.find_all(name='b')
    return [nombres_y_paises_lagos_pag, areas_lagos_pag]


def filtrar_nombres_paises_areas(divs_nombres_areas_paises):
    nombres_y_paises_lagos_pag = divs_nombres_areas_paises[0]
    areas_lagos_pag = divs_nombres_areas_paises[1]

    filtrar_nombres_y_paises = list(filter(lambda x: x.find(name='span'), nombres_y_paises_lagos_pag))

    nombres_lagos = [nombre.text.split(',')[0] for nombre in filtrar_nombres_y_paises]
    paises_lagos = [pais.text.split(',')[1] for pais in filtrar_nombres_y_paises]

    filtrar_areas_lagos = [re.findall(r'(\d+.\d+) (kilómetros cuadrados|km)', i.text) for i in areas_lagos_pag]
    areas_lagos = [area[0][0].replace('.', "") for area in filtrar_areas_lagos if area != []]

    nombres_paises_areas = [[nombres_lagos[i], paises_lagos[i], areas_lagos[i]] for i in range(len(paises_lagos))]
    return nombres_paises_areas


def extraer_coodernadas(lago, pais):

    url_geocode  = "https://geocode.maps.co/search?q={}"
    lago_url     = url_geocode.format(lago.replace(' ','+'))
    time.sleep(10)
    request_lago = requests.get(lago_url, headers={"Authorization" : "65ff1f1e5bafe015765365hvcee8ba9"})
    time.sleep(10)
    geocode_direcciones = {'direcciones':request_lago.json()}
    
    jsonpath_expr = parse('$..class')
    classes  = [match.value for match in jsonpath_expr.find(geocode_direcciones)]
    
    index_lago = [i for i in range(len(classes)) if classes[i] == 'natural' or classes[i] == 'tourism' or classes[i] == 'waterway' or classes[i] == 'water']
    
    if len(index_lago) == 0:
        latitude  = 'None'
        longitude = 'None'
    
    elif len(index_lago) > 0:
        
        latitude  = geocode_direcciones['direcciones'][index_lago[0]]['lat']
        longitude = geocode_direcciones['direcciones'][index_lago[0]]['lon']  
        print(latitude, longitude)
        
    return [latitude, longitude]


def extraer_datos(div_lago):
    nombre = div_lago[0]
    pais = div_lago[1]
    area = div_lago[2]
    print(nombre,pais)
    coordenadas = extraer_coodernadas(nombre,pais)

    return dict(nombre=nombre,
                pais=pais,
                area=area,
                latitud=coordenadas[0],
                longitude=coordenadas[1])

