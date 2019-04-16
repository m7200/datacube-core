"""
Generate extent (spatial and time) summaries for products in order to support queries
such as
    - What is the footprint of product 'ls8_level1_scene' for the month of 'November-2018'?
    - What is the footprint of product 'ls8_level1_scene'?
    - What is the footprint of product 'product_A' for region_code 'codeA' for month 'monthA'?

"""
from dateutil.rrule import rrule
from dateutil.relativedelta import relativedelta
from datetime import datetime
import ogr
from datacube import Datacube
from datacube.model import Range
from datacube.summary import SummaryAPI

import logging
import click

LOG = logging.getLogger(__name__)


@click.group(help=__doc__)
@click.option('--config', '-c', help="Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    """ Used to pass the datacube index to functions via click."""
    ctx.obj = Datacube(config=config).index


def compute_extent(summary, crs, **kwargs):

    time_footprint = []
    geom = ogr.Geometry(ogr.wkbMultiPolygon)

    datasets = summary.search_returing_datasets_light(**kwargs)  # ToDo
    for dataset in datasets:
        time_footprint.append(dataset.center_time)
        if dataset.extent:
            geom.AddGeometry(dataset.extent.to_crs(crs).__geo_interface__)
        else:
            LOG.info('Extent undefined for the dataset: %s', dataset.id)

    if not datasets:
        LOG.info('No datasets returned for query: %s', kwargs)

    return geom.UnionCascaded(), time_footprint


def compute_extent_periodic(index, product, freq, crs=None):

    # Get time bounds for the product
    summary = SummaryAPI(index)
    time_min = summary.get_product_time_min(product)
    time_max = summary.get_product_time_max(product)

    # The start date must be the first of the same month of time_min
    time_min_ = time_min - relativedelta(months=1)

    if not crs:
        # Find out product crs
        product_ = index.products.get_by_name(product)
        storage = product_.definition.get('storage')
        if storage is None:
            raise ValueError('crs is not specified for this product. It must be supplied')
        crs = storage.get('crs')
        if crs is None:
            raise ValueError('crs is not specified for this product. It must be supplied')

    month_start_list = rrule(freq, bymonthday=1, dtstart=time_min_, until=time_max)

    for time_range in _get_time_ranges(month_start_list):
        yield compute_extent(summary, crs, product=product, time=time_range)


def _get_time_ranges(date_list):

    for i, date in enumerate(date_list):
        if i == 0:
            prev = date
            continue

        yield Range(prev, date)
        prev = date
