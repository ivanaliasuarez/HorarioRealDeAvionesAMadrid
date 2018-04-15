# -*- coding: utf-8 -*-
"""
Created on Tue Mar 27 05:13:39 2018

@author: IvanA
"""

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
#from click.exceptions import ClickException

import pandas as pd
import csv

import urllib
from urllib import parse
from urllib import request
from urllib import robotparser
from datetime import datetime
import time

import re
import lxml.html
from lxml.cssselect import CSSSelector
import sys

from pathlib import Path

def main():
    # webdriver hay que cambiarlo para ajustarlo
    try:
        driver = webdriver.Chrome('C:\\Users\\ivana\\Documents\\Tools\\chromedriver.exe')

    except:
        print('Hay que indicar el path correcto del chromedriver.exe')
        exit

    else:
        seed_url = 'http://www.aena.es'
        
        user_agent='wswp'
            
        url = 'http://www.aena.es/csee/Satellite/infovuelos/es/'
        
        historicalcsv = 'informaciondevuelosacumulado.csv'
        
        try:
            dfhistorical_data = gethistorycsv(historicalcsv)
        
            rp = get_robots(seed_url)
               
            if rp.can_fetch(user_agent, url):
                dfvuelos = obtain_links(url,driver)
                dfnewdata = loop_through_links(rp, user_agent, dfvuelos, dfhistorical_data)   
 
        except:
            print('Error en función main')    

        finally:
            writerecordtocsv(dfnewdata,historicalcsv)
            driver.close()
    
def writerecordtocsv(dfnewdata,historicalcsv): 
    """Escribe los registros nuevos al archivo histórico"""
    
    #comprueba si el df está vacío
    if not dfnewdata.empty:
        myfile = Path(historicalcsv)
    
        if myfile.exists():
            with open(historicalcsv,'a', newline='') as originalfile:
                filewriter = csv.writer(originalfile)
                
                #escribe al archivo histórico todos los registros del df
                for ix, row in dfnewdata.iterrows():
                    filewriter.writerow(row)
        else:
            with open(historicalcsv,'w', newline='') as originalfile:
                filewriter = csv.writer(originalfile)
                
                #escribe al archivo histórico todos los registros del df
                for ix, row in dfnewdata.iterrows():
                    filewriter.writerow(row)
    

def gethistorycsv(historicalcsv):
    """Lee el archivo histórico de los vuelos en un dataframe que será utilizado
    más tarde para comprobar si la información de un vuelo ya se ha obtenido previamente"""
    

    myfile = Path(historicalcsv)
    
    if myfile.exists():
        colnames = ['fecha','vuelo', 'horaplaneada','horareal']
        dfhistoricalcsv = pd.read_csv(historicalcsv,header=None, names=colnames)
    else:
        dfhistoricalcsv = pd.DataFrame(columns=['fecha','vuelo','horaplaneada','horareal'])
        
    dfhistoricalcsv.set_index(['fecha','vuelo'])
         
    return dfhistoricalcsv        

    
def obtain_links(url,driver):
    """Obtiene los links destino para hacer el data scrape"""
    
    print('Abre página origen: ' , url)
    
    driver.get(url)
    
    elem = driver.find_element_by_xpath(
        './/input[@id="pagename"]'
        '/preceding-sibling::input[@type="hidden"]')
    
   
    driver.execute_script('''
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    ''', elem, 'L')
      
    print('Rellena los campos para hacer el crawling al aeropuerto de Madrid')
    #pone los parámetros  para pasar a la página de vuelos a Madrid
    driver.find_element_by_id('origin_ac').send_keys('MADRID-BARAJAS ADOLFO SUÁREZ (MAD )')
    
    driver.find_element_by_id('destiny_ac').clear()
    
    driver.find_element_by_id('destiny_ac').send_keys('Escribe origen')
    
    #Como hay dos botones de lupa se usa find_elements en vez de find_element
    #y se elige el segundo botón lupa con [1]
    driver.find_elements_by_class_name('btnLupa')[1].click()
         
    
    dfvuelos = pd.DataFrame(columns=['vuelo','paginaweb'])
    
    #Una vez hecho el crawling hasta la página que contiene la lista de vuelos
    #se hace un bucle para pasar por todas las páginas de vuelos
    #el bucle se termina cuando el link de página siguiente no existe
    print('Revisa todas las páginas de vuelos')
    i=0
    dfcounter = 0
    error_counter = 5
    while True:
        
        listelements = driver.find_elements_by_xpath("//*[@class='principal']//*[@class='col2']//a | //*[@class='principal par']//*[@class='col2']//a")
        for element in listelements:
            dfvuelos.loc[dfcounter,'vuelo'] = element.text
            dfvuelos.loc[dfcounter,'paginaweb'] = element.get_attribute("href")

            dfcounter += 1
            
        try:                                   
            print("pulso botón, voy a pagina: ", i)
            
            next_link = driver.find_element_by_xpath('//a[img/@src="/img/aena/ifv_flechaPagSiguiente.png"]')
            
            next_link.click()
            i = i + 1
                       
            time.sleep(5)            
            
        except NoSuchElementException:
            break
        
        except ElementNotVisibleException:
            break
       
        except StaleElementReferenceException:
            pass
        
        except WebDriverException as e:            
         
            if error_counter > 0:
                print('Going to sleep for 5 secs')
                error_counter = error_counter - 1
                time.sleep(5)
            else:
                raise ValueError('Error en función obtain_links: ') from e
            
        except:
            e = sys.exc_info()[0]
            raise ValueError('Error en función obtain_links: ') from e
            
          

       ## HAY QUE QUITAR ESTO #####################################
       ##PARA PRUEBAS LIMITAR A 1 ITERACIONES
       # if i == 1:
       #     break
       ###################################################### 
            
       
    return dfvuelos




def loop_through_links(rp,user_agent,dfvuelos, dfhistorical_data, delay=2, headers=None, proxy=None, num_retries=1):
    """"Comprueba todos los links recibidos en el df y obtiene la información de los vuelos"""
    
    
    #define dataframe para almacenar nuevos datos
    dfnewdata = pd.DataFrame(columns=['fecha','vuelo','horaplaneada','horareal'])   
    dfnewdata.set_index(['fecha','vuelo'])
    
    
    throttle = Throttle(delay)
    
    headers = headers or {}
    
    if user_agent:
        headers['User-agent'] = user_agent
        

    #repasa todos los vuelos obtenidos, aquellos que no existen y tienen 
    #la hora de llegada correctamente informada se almacenan para más tarde
    #escribirlos en el archivo histórico
    for ix, row in dfvuelos.iterrows():

        url= row[1]
        if rp.can_fetch(user_agent, url):
            try:
                throttle.wait(url)           
                html=download(url, headers, proxy, num_retries, data=None)
                scraped_data = scrape_data(html)
                
                #solo tratar los registros que tengan hora de llegada final     
                #construye un registro de vuelo - fecha, vuelo, hora planeada, hora real     
                
                flight_to_register = build_record_to_store(row[0], scraped_data)
                
                #comprueba si el no existe y tiene el horario de llegada final informado correctamente
                #si el registro existe o no esta informada la fecha el registro no se guarda
                if not check_if_record_exist(dfhistorical_data, flight_to_register) and "-1" not in flight_to_register[3]:
                    dfnewdata.loc[len(dfnewdata)]=flight_to_register
            except:
                e = sys.exc_info()[0]
                print('Error en función loop_through_links: ' ,e)
                break
                

    return dfnewdata
                
  
    
def check_if_record_exist(dfhistorical_data, flight_to_register):
    """Comprueba si un registro existe en el df histórico"""

    if dfhistorical_data.empty:
        return False
    else:
        #en caso de que encuentro un vuelo con misma fecha es que el registro existe en 
        #el archivo histórico
        if (dfhistorical_data.loc[(dfhistorical_data['fecha'] == flight_to_register[0]) & 
                                 (dfhistorical_data['vuelo'] == flight_to_register[1])]).empty:

           return False
        else:
           return True
              
    
    
    
def build_record_to_store(strflight, scrapped_data):
    """Recive la información obtenida de la web y la formatea para comparación y 
    almacenaje posterior"""
    try:
        print(scrapped_data)
        list_record_to_store = [scrapped_data[0], strflight, scrapped_data[1], get_final_hour(scrapped_data[5])]
       
        return list_record_to_store
    except:
        e = sys.exc_info()[0]
        raise ValueError('Error en función build_record_to_store') from e


def get_final_hour(final_hour):
    """Extrae la hora final del vuelo del string"""

    try:
        if "El vuelo ha aterrizado " in final_hour:
            list_time = re.findall(r'\b\d+\b', final_hour)
            hour = list_time[0] + ":" + list_time[1]   
            return hour
        else:
            return "-1"
    except:
        print('Input received: ', final_hour)
        return -1


def scrape_data(html):
    """Obtiene la información de los vuelos"""
    
    try:
        tree = lxml.html.fromstring(html)
    
    
        # construct a CSS Selector
        sel = CSSSelector('td')
        
        # Apply the selector to the DOM tree.
        results = sel(tree)
        
        # get the text out of all the results
        data = [result.text for result in results]
        
        return data
    
    except:
        e = sys.exc_info()[0]
        print('Error en función scrape_data',e)

    
    
   
class Throttle:
    """Ejecución de Throttle de descarga  """
    
    def __init__(self, delay):
       
        self.delay = delay
        
        self.domains = {}
        
    def wait(self, url):
        try:
            domain = parse.urlparse(url).netloc
            last_accessed = self.domains.get(domain)
    
            if self.delay > 0 and last_accessed is not None:
                sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
                if sleep_secs > 0:
                    time.sleep(sleep_secs)
            self.domains[domain] = datetime.now()
        except:
            e = sys.exc_info()[0]
            print('Error en Throttle - wait: ', e)



def get_robots(url):
    """Inicializa el robots parser para la página origen """
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(parse.urljoin(url, '/robots.txt'))
        print(parse.urljoin(url, '/robots.txt'))
        rp.read()
        return rp
    except IOError:
        print ('%s no está disponible' % url  )
    except:
        e = sys.exc_info()[0]
        print('Error en función get_robots', e)
        



def download(url, headers, proxy, num_retries, data=None):
    print ('Descargo página: ', url)
    
    try:
        myrequest = request.Request(url, data, headers)
        opener = request.build_opener()
    except:
        e = sys.exc_info()[0]
        print('Error en función download #1: ', e)
    
    if proxy:
        proxy_params = {parse.urlparse(url).scheme: proxy}
        opener.add_handler(request.ProxyHandler(proxy_params))
        
    try:      
        response = opener.open(myrequest)
        html = response.read().decode('utf-8')
        code = response.code
    except urllib.error.URLError as e:
        print('Download error:', e.code, e.reason)
     
        html = None
        if hasattr(e, 'code'):
            code = e.code
            if num_retries > 0 and ((500 <= code < 600) or code == 400):
                # retry 5XX HTTP errors
                return download(url, headers, proxy, num_retries-1, data)
        else:
            code = None
    except:
        e = sys.exc_info()[0]
        print('Error en función download #2:' , e)
    
    return html

###############################################################################
if __name__ == "__main__":
    main()
