import argparse
import pandas as pd
from models import CarArticle, Base, Phone, MetaInfo
from utils import setup_db, get_pln_rates, get_ukr_rates, get_cc_value
from datetime import date
import datetime
import xlsxwriter


def convert_currency(row, converter):
    currency = row['currency']
    value = row['value']

    # TODO: check this
    if currency.lower() == 'pln':
        return converter(value, 'eur')
    return value


def apply_pln_taxes(row):
    real_value = row['price_eur']

    if row['netto']:
        real_value *= 1.23

    if row['vat']:
        return real_value / 1.23
    return real_value


def calculate_urk_taxes(row, converter):
    value = row['price_eur']
    capacity = float(row['engine_capacity'])
    engine_type = row['engine_type']
    year = row['year'].year

    # TODO: check this
    if row['netto']:
        value = row['price_real']

    customs_clearing_value = get_cc_value(value, capacity, engine_type, year, converter)
    return customs_clearing_value


def calculate_total(row):
    return row['price_real'] + row['customs_clearing']

# session = setup_db()
# DATA = pd.read_sql(session.query(CarArticle).statement, session.bind)
#
# # data pre-processing
# DATA['value'] = DATA['value'].astype('float')
# DATA['price_eur'] = DATA.apply(convert_currency, axis=1)
# DATA['price_real'] = DATA.apply(apply_pln_taxes, axis=1)
# DATA['customs_clearing'] = DATA.apply(calculate_urk_taxes, axis=1)
# DATA['price_total'] = DATA.apply(calculate_total, axis=1)
#
# DB_META = session.query(MetaInfo).get(1)


def args_init():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--value_min', required=False,
                            default=0, help='Car minimum value')
    arg_parser.add_argument('--value_max', required=False,
                            default=0, help='Car maximum value')
    arg_parser.add_argument('--year_from', required=False,
                            default='1990', help='Production year - from')
    arg_parser.add_argument('--year_to', required=False,
                            default=str(date.today().year), help='Production year - to')
    arg_parser.add_argument('--mileage_min', required=False,
                            default=0, help='Minimum mileage')
    arg_parser.add_argument('--mileage_max', required=False,
                            default=0, help='Maximum mileage')
    arg_parser.add_argument('--fuel_type', required=False,
                            default='', help='Engine fuel type')
    arg_parser.add_argument('--damaged', required=False,
                            default=0, help='Add damaged cars to search query')

    args = arg_parser.parse_args()

    args.year_from = datetime.datetime.strptime(args.year_from, '%Y')
    args.year_to = datetime.datetime.strptime(args.year_to, '%Y')

    return args


def color_row(workbook, format_class, color=None):
    format = workbook.add_format()
    for attr, val in format_class.__dict__.items():
        setattr(format, attr, val)
    if color:
        format.bg_color = color
    return format


def export_xlsx(df):
    filename = datetime.datetime.now().strftime("%d.%m.%Y_%H.%M") + '_otomoto.xlsx'

    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet('page#1')

    format_title = workbook.add_format(
        {
            'bold': True,
            'align': 'center',
            'valign': 'vcenter'
         }
    )
    format_normal = workbook.add_format(
        {
        'align': 'center',
        'valign': 'vcenter'
        }
    )

    format_numeric = workbook.add_format(
        {
            'num_format': '#.0#',
            'align': 'center',
            'valign': 'vcenter'
        }
    )
    format_capacity = workbook.add_format(
        {
            'num_format': '0.0#',
            'align': 'center',
            'valign': 'vcenter'
        }
    )
    format_datetime = workbook.add_format(
        {
            'num_format': 'yy-mm-dd hh:mm:ss',
            'align': 'center',
            'valign': 'vcenter'
        }
    )

    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 15)
    worksheet.set_column('C:C', 12)
    worksheet.set_column('D:D', 15)
    worksheet.set_column('E:E', 12)
    worksheet.set_column('F:F', 15)
    worksheet.set_column('G:G', 20)
    worksheet.set_column('H:H', 12)
    worksheet.set_column('I:I', 12)
    worksheet.set_column('J:J', 12)
    worksheet.set_column('K:K', 12)
    worksheet.set_column('L:L', 12)
    worksheet.set_column('M:M', 20)
    worksheet.set_column('N:N', 20)
    worksheet.set_column('O:O', 20)
    worksheet.set_column('P:P', 20)
    worksheet.set_column('Q:Q', 20)
    worksheet.set_column('R:R', 12)
    worksheet.set_column('S:S', 20)

    worksheet.write('A1', 'Name', format_title)
    worksheet.write('B1', 'Manufacturer', format_title)
    worksheet.write('C1', 'Year', format_title)
    worksheet.write('D1', 'Mileage', format_title)
    worksheet.write('E1', 'Engine cap.', format_title)
    worksheet.write('F1', 'Engine type', format_title)

    worksheet.write('G1', 'Value', format_title)
    worksheet.write('H1', 'Currency', format_title)

    worksheet.write('I1', 'Negotiation', format_title)
    worksheet.write('J1', 'Netto', format_title)
    worksheet.write('K1', 'Brutto', format_title)
    worksheet.write('L1', 'VAT', format_title)

    worksheet.write('M1', 'Price (EUR)', format_title)
    worksheet.write('N1', 'Price (EUR, VAT refund)', format_title)
    worksheet.write('O1', 'Customs fee (EUR)', format_title)
    worksheet.write('P1', 'Total price', format_title)

    worksheet.write('Q1', 'Location', format_title)
    worksheet.write('R1', 'Link', format_title)
    worksheet.write('S1', 'DB date', format_title)

    index = 2
    for _, row in df.iterrows():
        # TODO: replace this with index generating function

        color = None
        if DB_META.last_start < row['record_created']:
            color = '#00C7CE'

        worksheet.write('A{}'.format(index), row['name'], color_row(workbook, format_normal, color=color))
        worksheet.write('B{}'.format(index), row['manufacturer'], color_row(workbook, format_normal, color=color))
        worksheet.write('C{}'.format(index), row['year'], color_row(workbook, format_normal, color=color))
        worksheet.write_number('D{}'.format(index), row['mileage'], color_row(workbook, format_normal, color=color))
        worksheet.write('E{}'.format(index), row['engine_capacity'], color_row(workbook, format_capacity, color=color))
        worksheet.write('F{}'.format(index), row['engine_type'], color_row(workbook, format_normal, color=color))

        worksheet.write('G{}'.format(index), row['value'], color_row(workbook, format_numeric, color=color))
        worksheet.write('H{}'.format(index), row['currency'], color_row(workbook, format_normal, color=color))

        worksheet.write_boolean('I{}'.format(index), row['negotiation'], color_row(workbook, format_normal, color=color))
        worksheet.write_boolean('J{}'.format(index), row['netto'], color_row(workbook, format_normal, color=color))
        worksheet.write_boolean('K{}'.format(index), row['brutto'], color_row(workbook, format_normal, color=color))
        worksheet.write_boolean('L{}'.format(index), row['vat'], color_row(workbook, format_normal, color=color))

        worksheet.write('M{}'.format(index), row['price_eur'], color_row(workbook, format_numeric, color=color))
        worksheet.write('N{}'.format(index), row['price_real'], color_row(workbook, format_numeric, color=color))
        worksheet.write('O{}'.format(index), row['customs_clearing'], color_row(workbook, format_numeric, color=color))
        worksheet.write('P{}'.format(index), row['price_total'], color_row(workbook, format_numeric, color=color))

        worksheet.write('Q{}'.format(index), row['location'], color_row(workbook, format_normal, color=color))
        worksheet.write_url('R{}'.format(index), row['link'], color_row(workbook, format_normal, color=color), string='link')
        worksheet.write_datetime('S{}'.format(index), row['record_created'], color_row(workbook, format_datetime, color=color))

        index += 1

    workbook.close()

# export_xlsx(DATA)

if __name__ == '__main__':
    args = args_init()

    cur_ukr = get_ukr_rates()
    cur_pln = get_pln_rates()

    session = setup_db()
    data = pd.read_sql(session.query(CarArticle).statement, session.bind)

    # data pre-processing
    data['value'] = data['value'].astype('float')
    data['price_eur'] = data.apply(convert_currency, args=(cur_pln, ), axis=1)
    data['price_real'] = data.apply(apply_pln_taxes, axis=1)
    data['customs_clearing'] = data.apply(calculate_urk_taxes, args=(cur_ukr, ), axis=1)
    data['price_total'] = data.apply(calculate_total, axis=1)
    data['year'] = data['year'].apply(lambda x: x.year)

    DB_META = session.query(MetaInfo).get(1)

    data = data.query('{} <= value'.format(args.value_min))
    if args.value_max:
        # val_max = data['value'].max()
        # data['value'] = data[(0 <= data['value']) & (data['value'] <= val_max)]['value']
        data = data.query('{} <= value <= {}'.format(args.value_min, args.value_max))

    data = data.query('{} <= year <= {}'.format(args.year_from.year, args.year_to.year))
    data = data.query('{} <= mileage <= {}'.format(args.mileage_min, args.mileage_max))
    print(data.head())