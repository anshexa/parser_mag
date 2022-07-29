# coding: utf-8
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import json
import time
import requests
from lxml import html
import re
import urllib.parse
from random import randint
import psycopg2.extras

exception_lst = [
    '',
]

exception_lst_DIN = [
    '',
]


def request_f(url, retry=1000, json=''):
    try:
        r = requests.request("GET", url)
        # если переход на другой сайт
        if os.environ['OTHER_DOMAIN'] in r.url:
            return r
        if json == 'json':
            return r
        assert (r.status_code == 200), ("Error, Response code: ", r.status_code, url)
    except Exception as _e:
        if retry:
            print('FAIL: %r generated an exception: %s' % (urllib.parse.unquote_plus(url), _e))
            time.sleep(randint(5, 10))
            return request_f(url, retry=(retry - 1))  # retry fail
        else:
            print("\n retry limit is null \n")
            raise  # fail
    else:
        return r  # ok


connect = psycopg2.connect(database=os.environ['NAME_DB'],
                           user=os.environ['USER_DB'],
                           host=os.environ['HOST_DB'],
                           password=os.environ['PASSWORD_DB'],
                           keepalives=1,
                           keepalives_idle=30,
                           keepalives_interval=10,
                           keepalives_count=5
                           )
connect.set_client_encoding('UTF8')
connect.set_session(autocommit=True)
cursor = connect.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("CREATE TABLE IF NOT EXISTS struct "
               "(id serial NOT NULL, breadcrumb TEXT, bread_specific VARCHAR(8), name TEXT, link TEXT, "
               "id_ VARCHAR(200), filters TEXT)")

cursor.execute("CREATE TABLE IF NOT EXISTS products "
               "(id serial NOT NULL, id_prod VARCHAR(200), name TEXT, link TEXT, "
               "img TEXT, image360 TEXT, video TEXT, "
               "docs1 TEXT, characteristics TEXT, docs2 TEXT, "
               "software TEXT, related_prod TEXT)")


def rec_to_file(nameFile, string):
    my_file = open(nameFile, 'a', encoding='utf-8')
    my_file.write("{}\n".format(string))
    my_file.close()


def extract_breadcrump(tree, level, href):
    # извлечение хлебных крошек
    bread_lst = []
    bread = tree.xpath('//nav[contains(@class,"breadcrumbs")]//li')
    if len(bread) > 0:
        for li in bread:
            el_lst = li.xpath('.//text()')
            br_nm_lst = []
            for el in el_lst:
                el_val = re.sub('[\t\r\n]', '', el)
                if len(el_val) > 0:
                    br_nm_lst.append(el_val)
            br_nm = ''.join(br_nm_lst)
            br_nm = br_nm.lstrip().rstrip()
            bread_lst.append(br_nm)
        bread = '###'.join(bread_lst)
        if len(bread_lst) == level + 2:
            return bread, ""
        else:
            rec_to_file('error_bread.txt', href)
            return bread, "specific"
    return ""


def exist_bread(bread):
    # проверка наличия хлебных крошек в бд
    bread = re.sub("'", "''", bread)
    cursor.execute(f"SELECT * FROM struct WHERE breadcrumb='{bread}' LIMIT 1")
    br = cursor.fetchall()
    if len(br) > 0:
        return 'exist'
    else:
        return 'not_exist'


def exist_product(id_prod):
    # проверка наличия товара в бд
    cursor.execute(f"SELECT * FROM products WHERE id_prod='{id_prod}' LIMIT 1")
    pr = cursor.fetchall()
    if len(pr) > 0:
        return 'exist'
    else:
        return 'not_exist'


def pars_product(url_prod):
    # парсинг товара
    response9 = request_f(url_prod, 1000, '')
    html_text = response9.text
    text = html_text.replace("</br>", "<br/>")
    tree9 = html.fromstring(text)

    level5 = 5
    # хлебные крошки
    breadcrumb, bread_specific = extract_breadcrump(tree9, level5, url_prod)

    # id товара
    id_prod = tree9.xpath('//div[@class="pdp-product-info__id"]/text()')
    if len(id_prod) > 0:
        id_prod = id_prod[0].lstrip().rstrip()

    # название товара
    nm_prod = tree9.xpath('//h2[@class="pdp-product-info__description"]/text()')
    if len(nm_prod) > 0:
        nm_prod = nm_prod[0].lstrip().rstrip()

    rec_db_struct(bread_specific, breadcrumb, id_prod, nm_prod, url_prod)

    # сохраняем товар в бд, если не сохранен
    exist_prod = exist_product(id_prod)
    if exist_prod == 'not_exist':
        # картинки
        url_media = "{}/ru/ru/product/pdp/media/{}".format(os.environ['BASE_URL'], id_prod)
        response10 = request_f(url_media, 1000, 'json')
        img = ''
        image360 = ''
        video_list = []
        if response10.status_code == 200:
            # одно изображение
            media_json = json.loads(response10.text)
            if 'zoomPictureDesktop' in media_json:
                img = media_json['zoomPictureDesktop']['url']
            if 'image360' in media_json:
                image360 = media_json['image360']['image360Url']

            # видео
            video_l = media_json['gallery']['videoGroups']
            if len(video_l) > 0:
                video_l = media_json['gallery']['videoGroups'][0]['videoItems']
                for vid in video_l:
                    video_list.append(vid['videoUrl'])

        # документы1
        docs1_list = {}
        docs1_l = tree9.xpath('//div[@id="product-infos"]//a')
        if len(docs1_l) > 0:
            for doc in docs1_l:
                nm_doc = doc.xpath('./text()')[0]
                nm_doc = nm_doc.lstrip().rstrip()
                href_doc = doc.xpath('./@href')[0]
                href_doc = str(href_doc)
                docs1_list[nm_doc] = href_doc

        # характеристики
        product_charact = {}
        charact_table = tree9.xpath('//li[@id="characteristics"]//table')
        if len(charact_table) > 0:
            for block in charact_table:
                head = block.xpath('./caption/text()')
                head = str(head[0])
                product_charact[head] = []
                rows = block.xpath('./tbody/tr')
                for r in rows:
                    val_list = []
                    key = r.xpath('./th/text()')
                    key = str(key[0])
                    val = r.xpath('./td//text()')
                    for v in val:
                        vv = v.lstrip().rstrip()
                        if len(vv) > 0:
                            val_list.append(vv)
                    product_charact[head].append({key: val_list})

        # документы2
        docs2_list = {}
        doc2_url_params = tree9.xpath('//li[@id="pdp-documents"]')
        if len(doc2_url_params) > 0:
            docs2_list = extr_list_doc2_soft(doc2_url_params, 'doc')

        # программы
        software_list = {}
        software_url_params = tree9.xpath('//li[@id="pdp-software"]')
        if len(software_url_params) > 0:
            software_list = extr_list_doc2_soft(software_url_params, 'soft')

        # рекомендуемые товары
        related_prod_list = {}
        url_r11 = '{}/ru/ru/product/api/related-products/{}?site-type=b2b'.format(os.environ['BASE_URL'], id_prod)
        response11 = request_f(url_r11, 1000, 'json')
        if len(response11.text) > 0 and 'info' in response11.text and response11.status_code == 200:
            rel_lst = json.loads(response11.text)['info']
            if len(rel_lst) > 0:
                for rel in rel_lst:
                    sect_rel = rel['viewProductLabel']
                    related_prod_list[sect_rel] = []
                    rel_prods_lst = rel['products']
                    for rel_p in rel_prods_lst:
                        rel_id = rel_p['product']['skuId']
                        rel_url = rel_p['url']
                        related_prod_list[sect_rel].append({rel_id: rel_url})
        pass

        cursor.execute('INSERT INTO products (id_prod, name, link, '
                       'img, image360, video, '
                       'docs1, characteristics, docs2, '
                       'software, related_prod) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                       (id_prod, nm_prod, url_prod,
                        img, image360, video_list,
                        json.dumps(docs1_list), json.dumps(product_charact), json.dumps(docs2_list),
                        json.dumps(software_list), json.dumps(related_prod_list)))


def rec_db_struct(breadcrumb, bread_spec, nm, link, id_):
    # запись в бд, если хлебные крошки не записаны
    ex_bread = exist_bread(breadcrumb)
    if ex_bread == 'not_exist':
        cursor.execute('INSERT INTO struct (breadcrumb, bread_specific, name, link, id_) VALUES (%s, %s, %s, %s, %s);',
                       (breadcrumb, bread_spec, nm, link, id_))


def extr_list_doc2_soft(url_params, entity):
    # извлечение списка докуметов2 или ПО товара
    data_prod_id = str(url_params[0].xpath('./div[@class="js-content-placeholder"]/@data-product-id')[0])
    data_range_id = str(url_params[0].xpath('./div[@class="js-content-placeholder"]/@data-range-id')[0])
    data_filter = str(url_params[0].xpath('./div[@class="js-content-placeholder"]/@data-filter-for-tab')[0])
    data_head = str(url_params[0].xpath('./div[@class="js-content-placeholder"]/@data-heading')[0])
    data_block_id = str(url_params[0].xpath('./div[@class="js-content-placeholder"]/@data-block-id')[0])
    # запрос документов или ПО
    url_doc2 = "{}/ru/ru/product/async/productDocuments.jsp?productId={}&paramRangeId={}&filterForTab={}&heading={}&blockId={}".format(
        os.environ['BASE_URL'], data_prod_id, data_range_id, data_filter, data_head, data_block_id)
    response12 = request_f(url_doc2, 1000, '')
    entities_list = {}
    if len(response12.text) > 0:
        tree12 = html.fromstring(response12.text)
        ent_lst = ''
        if entity == 'doc':
            # документы на русском
            ent_lst = tree12.xpath('//div[@class="docs-table js-docs-table"]/div[contains(@class,"docs-table__section")]//div[contains(@data-lang,"ru,")]')
        if entity == 'soft':
            # ПО и прошивки на любом языке
            ent_lst = tree12.xpath('//div[@class="docs-table js-docs-table"]/div[contains(@class,"docs-table__section")]//div[contains(@class,"js-sortable-item")]')
        if len(ent_lst) > 0:
            for ent in ent_lst:
                sect = ent.xpath('../..//div[@class="docs-table__head"]/div[contains(@class,"docs-table__column-name")]/text()')[0].lstrip().rstrip()
                if sect not in entities_list:
                    entities_list[sect] = []
                href_e = ent.xpath('.//span[@class="docs-table__link-text"]/a/@href')
                href_e = str(href_e[0])
                nm_e = ent.xpath('.//span[@class="docs-table__link-text"]/a/text()')
                nm_e = re.sub('[\t\r\n]', '', nm_e[0])
                entities_list[sect].append({nm_e: href_e})
    return entities_list


def extract_subnode(tree4):
    # поиск подузлов
    subnode_list = []
    subnode = tree4.xpath('//ul[@class="subnode"]')
    if len(subnode) > 0:
        subnode_l = subnode[0].xpath('./li[@class="subnode__item"]')
        for subn in subnode_l:
            href_subn = str(subn.xpath('./a[@class="subnode__link"]/@href')[0])
            subnode_list.append(href_subn)
    return subnode_list


base_url = "{}/ru/ru/".format(os.environ['BASE_URL'])
response = request_f(base_url, 1000, '')
tree = html.fromstring(response.text)
group_lst = tree.xpath('//div[@class="sdl-header-se_mm-main-list-products"]//li[@class="sdl-header-se_mm-l2-item"]')
for gr in group_lst:
    # название раздела
    nm_gr_lvl1 = gr.xpath('.//a[contains(@class,"sdl-header-se_mm-l2-link")]//text()')
    if len(nm_gr_lvl1) > 0:
        nm_gr_lvl1 = ''.join(nm_gr_lvl1)
        nm_gr_lvl1 = nm_gr_lvl1.lstrip().rstrip()

    # ссылка раздела
    href_gr_lvl1 = gr.xpath('.//a[contains(@class,"sdl-header-se_mm-l2-link")]/./@href')
    if len(href_gr_lvl1) > 0:
        href_gr_lvl1 = str(href_gr_lvl1[0])

    # запись в бд, если ссылка не записана
    cursor.execute(f"SELECT * FROM struct WHERE link='{href_gr_lvl1}' LIMIT 1")
    hr = cursor.fetchall()
    if len(hr) > 0:
        pass
    else:
        cursor.execute('INSERT INTO struct (name, link) VALUES (%s, %s);',
                       (nm_gr_lvl1, href_gr_lvl1))

    # подразделы
    gr_lvl2 = gr.xpath('.//a[contains(@class,"sdl-header-se_mm-l3-link")]')
    for gr_l2 in gr_lvl2:
        # название подраздела
        nm_gr_lvl2 = gr_l2.xpath('./text()')
        if len(nm_gr_lvl2) > 0:
            nm_gr_lvl2 = nm_gr_lvl2[0].lstrip().rstrip()

        # ссылка подраздела
        href_gr_lvl2 = gr_l2.xpath('./@href')
        if len(href_gr_lvl2) > 0:
            href_gr_lvl2 = str(href_gr_lvl2[0])

        if href_gr_lvl2 not in exception_lst:
            # заходим в подраздел
            response2 = request_f(href_gr_lvl2, 1000, '')
            tree2 = html.fromstring(response2.text)
            level2 = 2
            # хлебные крошки
            bread_gr_lvl2, bread_specific_lvl2 = extract_breadcrump(tree2, level2, href_gr_lvl2)

            rec_db_struct(bread_gr_lvl2, bread_specific_lvl2, nm_gr_lvl2, href_gr_lvl2, '')

            # подкатегории
            gr_lvl3 = tree2.xpath('//section[@class="subcategory"]')
            for gr_l3 in gr_lvl3:
                # id подкатегории
                id_gr_l3 = gr_l3.xpath('./@data-id')
                id_gr_l3 = str(id_gr_l3[0])

                # название подкатегории
                nm_gr_lvl3 = gr_l3.xpath('.//h2/a/text()')
                if len(nm_gr_lvl3) > 0:
                    nm_gr_lvl3 = ''.join(nm_gr_lvl3)
                    nm_gr_lvl3 = nm_gr_lvl3.lstrip().rstrip()

                # ссылка подкатегории (подкатегории встречаются в нескольких подразделах)
                href_gr_lvl3 = gr_l3.xpath('.//h2/a/@href')
                if len(href_gr_lvl3) > 0:
                    href_gr_lvl3 = str(href_gr_lvl3[0])

                if href_gr_lvl3 not in exception_lst_DIN:
                    # заходим в подкатегорию
                    url_r3 = '{}{}'.format(os.environ['BASE_URL'], href_gr_lvl3)
                    response3 = request_f(url_r3, 1000, '')
                    tree3 = html.fromstring(response3.text)
                    level3 = 3
                    # хлебные крошки
                    bread_gr_lvl3, bread_specific_lvl3 = extract_breadcrump(tree3, level3, href_gr_lvl3)

                    rec_db_struct(bread_gr_lvl3, bread_specific_lvl3, nm_gr_lvl3, href_gr_lvl3, id_gr_l3)

                    # секции
                    gr_lvl4 = tree3.xpath('//a[@class="subcategory-section-range__link"]')
                    for gr_l4 in gr_lvl4:
                        # название секции
                        nm_gr_lvl4 = gr_l4.xpath('./h4//text()')
                        if len(nm_gr_lvl4) > 0:
                            nm_gr_lvl4 = ''.join(nm_gr_lvl4)
                            nm_gr_lvl4 = nm_gr_lvl4.lstrip().rstrip()

                        # ссылка секции
                        href_gr_lvl4 = gr_l4.xpath('./@href')
                        if len(href_gr_lvl4) > 0:
                            href_gr_lvl4 = str(href_gr_lvl4[0])

                        def work_with_gr_lvl4(href_gr_lvl4):
                            # работа с 4 уровнем (секции)
                            response4 = request_f(href_gr_lvl4, 1000, '')
                            # если был переход на другой сайт
                            if os.environ['OTHER_DOMAIN'] in response4.url:
                                return
                            tree4 = html.fromstring(response4.text)
                            level4 = 4
                            # хлебные крошки
                            bread_gr_lvl4, bread_specific_lvl4 = extract_breadcrump(tree4, level4, href_gr_lvl4)

                            nm_gr_lvl4 = tree4.xpath('//h1/text()')
                            if len(nm_gr_lvl4) > 0:
                                nm_gr_lvl4 = str(nm_gr_lvl4[0])

                            descr_h2 = tree4.xpath('//div[@class="left-column"]/h2/text()')
                            if len(descr_h2) > 0:
                                descr_h2 = str(descr_h2[0])

                            # фильтрация товаров
                            filter_prod_list = []
                            used_filt_url = []

                            def extract_filter(href_gr_lvl4, base_filt_url, filt_exist, single_filt):
                                https = ''
                                n_param = ''
                                and_param = ''
                                if '/ru/ru/' not in href_gr_lvl4:
                                    # то это параметр, выделяем N
                                    if re.search('(?<=[\?N=])(.*?)(?=&)', href_gr_lvl4) is not None:
                                        n_param = re.search('(?<=[\?N=])(.*?)(?=&)', href_gr_lvl4).group(0)
                                        and_param = '&'
                                elif os.environ['BASE_URL'] not in href_gr_lvl4:
                                    https = os.environ['BASE_URL']
                                    base_filt_url = href_gr_lvl4
                                    base_filt_url = urllib.parse.unquote_plus(base_filt_url)
                                else:
                                    base_filt_url = href_gr_lvl4
                                    base_filt_url = urllib.parse.unquote_plus(base_filt_url)
                                if os.environ['BASE_URL'] not in base_filt_url:
                                    https = os.environ['BASE_URL']
                                # собираем ссылку и если ее нет - записываем
                                count_filt_url = '{}{}{}{}'.format(https, base_filt_url, and_param, n_param)
                                count_filt_url = urllib.parse.unquote_plus(count_filt_url)
                                if count_filt_url not in filter_prod_list:
                                    if filt_exist != 'filt_not_exist':
                                        filter_prod_list.append(count_filt_url)
                                    if single_filt != 1:
                                        filter_link = count_filt_url.replace("product-range", "product-range-refinements")
                                        url_r5 = '{}&format=json&ts=1625527189530'.format(filter_link)
                                        response5 = request_f(url_r5, 1000, 'json')
                                        name_filter_list_lvl4 = []
                                        if response5.status_code == 200:
                                            if json.loads(response5.text)['SecondaryContent'] is not None:
                                                filter_lst = json.loads(response5.text)['SecondaryContent'][1]['contents'][0]['navigation']
                                                if len(filter_lst) > 0:
                                                    filt_exist = 'filt_exist'
                                                    base_filts = filter_lst[0]['refinements']
                                                    for filter in reversed(filter_lst):
                                                        name_filter = filter['characteristicName']
                                                        name_filter_list_lvl4.append(name_filter)
                                                        single_filt = 0
                                                        # находим фильтры, влияющие на отображаемые товары
                                                        if filter['subCharacteristicName'] is None:
                                                            filt_cat_items = filter['refinements']
                                                            if len(filt_cat_items) == 1:
                                                                single_filt = 1
                                                            for cat_item in reversed(filt_cat_items):
                                                                lbl_filt_cat = cat_item['label']
                                                                url_filt_cat = cat_item['navigationState']
                                                                checked = cat_item['properties']['checked']

                                                                url_filt_cat = urllib.parse.unquote_plus(url_filt_cat)

                                                                if checked == 'false' and url_filt_cat not in used_filt_url:
                                                                    extract_filter(url_filt_cat, base_filt_url, filt_exist, single_filt)
                                                                    used_filt_url.append(url_filt_cat)

                                                                # для одиночного фильтра
                                                                if single_filt == 1 and url_filt_cat not in used_filt_url:
                                                                    extract_filter(url_filt_cat, base_filt_url, filt_exist, single_filt)
                                                                    used_filt_url.append(url_filt_cat)

                            extract_filter(href_gr_lvl4, '', 'filt_not_exist', 0)

                            rec_db_struct(bread_gr_lvl4, bread_specific_lvl4, nm_gr_lvl4, href_gr_lvl4, '')

                            # товары
                            full_prod_list = []

                            def extract_prod_list(tree4):
                                prod_cards = tree4.xpath('//div[contains(@class,"product-list-wrapper__sub-wrapper")]')
                                if len(prod_cards) > 0:
                                    product_ids = prod_cards[0].xpath('//product-cards-wrapper/@product-ids')
                                    if len(product_ids) > 0:
                                        product_ids = str(product_ids[0])
                                        # запрос к апи
                                        url_r6 = '{}/ru/ru/product/api/productCard/main?ids={}'.format(os.environ['BASE_URL'], product_ids)
                                        response6 = request_f(url_r6, 1000, 'json')
                                        product_url_params = str(prod_cards[0].xpath('//product-cards-wrapper/@product-url-params')[0])
                                        if response6.status_code == 200:
                                            prod_lst = json.loads(response6.text)['products']
                                            for prod in prod_lst:
                                                # чистый урл товара без параметров
                                                href_prod = prod['pdpUrl']
                                                work_link = '{}{}'.format(href_prod, product_url_params)
                                                work_link = urllib.parse.unquote_plus(work_link)
                                                print('/_/ link processed       {}'.format(work_link))
                                                full_prod_list.append('{}{}'.format(href_prod, product_url_params))
                                        # если есть пагинация
                                        contains_pagin = tree4.xpath('//div[@class="bottom-navigation"]')
                                        if len(contains_pagin) > 0:
                                            pagin_next = contains_pagin[0].xpath('.//a[contains(@data-arrow-label,"Далее")]')
                                            if len(pagin_next) > 0:
                                                pagin_next_param = str(pagin_next[0].xpath('./@href')[0])
                                                num_next_page = re.search('(?<=&No=)(.*?)(?=&)', pagin_next_param).group(0)
                                                next_param = re.search('(?=&[No=])(.*)', pagin_next_param).group(0)
                                                n_param = re.search('(?<=[\?N=])(.*?)(?=&)', pagin_next_param).group(0)
                                                pagin_next_url_lvl4 = '{}&{}{}'.format(href_gr_lvl4, n_param, next_param)
                                                # заходим в секцию (на следующую стр)
                                                response8 = request_f(pagin_next_url_lvl4, 1000, '')
                                                tree8 = html.fromstring(response8.text)
                                                extract_prod_list(tree8)
                                            pass

                            # поиск товара по фильтрам. если фильтра нет - ищем товары на данной странице
                            if len(filter_prod_list) > 0:
                                for filt_url in filter_prod_list:
                                    response44 = request_f(filt_url, 1000, '')
                                    tree44 = html.fromstring(response44.text)
                                    level4 = 4
                                    extract_prod_list(tree44)
                            else:
                                extract_prod_list(tree4)
                            for prod in full_prod_list:
                                pars_product(prod)

                            # подузлы
                            subnode_list = extract_subnode(tree4)
                            if len(subnode_list) > 0:
                                for subn_url in subnode_list:
                                    # заходим в подузел как в 4 уровень
                                    work_with_gr_lvl4(subn_url)

                        work_with_gr_lvl4(href_gr_lvl4)
    pass


print('Finish')
