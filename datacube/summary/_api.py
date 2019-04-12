from typing import Optional
import json
import logging
from collections import namedtuple

from sqlalchemy import select, func
from datacube.index import Index, fields
from datacube.model import Dataset
from datacube.drivers.postgres._fields import DateDocField, NativeField, SimpleDocField
from datacube.drivers.postgres._schema import DATASET
from datacube.utils import geometry, cached_property

from ._schema import PRODUCT_SUMMARIES

_LOG = logging.getLogger(__name__)


class DatasetSpatial(object):
    def __init__(self, uuid, grid_spatial):
        self._gs = grid_spatial
        self.id = uuid

    @property
    def crs(self) -> Optional[geometry.CRS]:
        """ Return CRS if available
        """

        return Dataset.crs.__get__(self)

    @property
    def extent(self) -> Optional[geometry.Geometry]:
        """ :returns: valid extent of the dataset or None
        """

        return Dataset.extent.__get__(self, DatasetSpatial)


class SummaryAPI:  # pylint: disable=protected-access
    def __init__(self, index: Index):
        self._index = index
        # self._engine = index._db._engine  # pylint: disable=protected-access

    def get_summaries(self, **kwargs):
        return self._index._db._engine.execute(
            select(
                [PRODUCT_SUMMARIES]
            ).where(
                PRODUCT_SUMMARIES.c.query == kwargs
            )
        ).first()

    def get_product_time_min(self, product: str):

        # Get the offsets of min time in dataset doc
        product = self._index.products.get_by_name(product)
        dataset_section = product.metadata_type.definition['dataset']
        min_offset = dataset_section['search_fields']['time']['min_offset']

        time_field = DateDocField('aquisition_time_min',
                                  'Min of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,  # is it indexed ToDo
                                  offset=min_offset,
                                  selection='least')

        result = self._index._db._engine.execute(
            select([func.min(time_field.alchemy_expression)]).where(
                DATASET.c.dataset_type_ref == product.id
            )
        ).first()

        return result[0]

    def get_product_time_max(self, product: str):

        # Get the offsets of min time in dataset doc
        product = self._index.products.get_by_name(product)
        dataset_section = product.metadata_type.definition['dataset']
        max_offset = dataset_section['search_fields']['time']['max_offset']

        time_field = DateDocField('aquisition_time_max',
                                  'Max of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,  # is it indexed ToDo
                                  offset=max_offset,
                                  selection='greatest')

        result = self._index._db._engine.execute(
            select([func.max(time_field.alchemy_expression)]).where(
                DATASET.c.dataset_type_ref == product.id
            )
        ).first()

        return result[0]

    # pylint: disable=too-many-locals
    def search_returing_spatial(self, field_names=None, limit=None, **query):

        if field_names:
            result_type = namedtuple('search_result', ('grid_spatial',) + tuple(field_names))
        else:
            result_type = namedtuple('search_result', ('grid_spatial',))

        for product, query_exprs in self.make_query_expr(query):

            dataset_section = product.metadata_type.definition['dataset']
            grid_spatial = dataset_section.get('grid_spatial')

            if not grid_spatial:
                continue

            dataset_fields = product.metadata_type.dataset_fields
            grid_spatial_field = SimpleDocField(
                'grid_spatial', 'grid_spatial', DATASET.c.metadata,
                False,
                offset=dataset_section.get('grid_spatial') or ['grid_spatial']
            )

            select_fields = [grid_spatial_field]
            if field_names:
                for field_name in field_names:
                    assert dataset_fields.get(field_name)
                    select_fields.append(dataset_fields[field_name])

            with self._index._db.connect() as connection:
                results = connection.search_datasets(
                    query_exprs,
                    select_fields=select_fields,
                    limit=limit
                )
            for result in results:
                if field_names and 'id' in field_names:
                    uuid = result[field_names.index('id') + 1]
                else:
                    uuid = None
                yield result_type(DatasetSpatial(uuid, json.loads(result[0])), *result[1:])

    def make_source_expr(self, source_filter):

        assert source_filter

        product_queries = list(self._index.datasets._get_product_queries(source_filter))
        if not product_queries:
            # No products match our source filter, so there will be no search results regardless.
            raise ValueError('No products match source filter: ' % source_filter)
        if len(product_queries) > 1:
            raise RuntimeError("Multi-product source filters are not supported. Try adding 'product' field")

        source_queries, source_product = product_queries[0]
        dataset_fields = source_product.metadata_type.dataset_fields

        return tuple(fields.to_expressions(dataset_fields.get, **source_queries))

    def make_query_expr(self, query):

        product_queries = list(self._index.datasets._get_product_queries(query))
        if not product_queries:
            raise ValueError('No products match search terms: %r' % query)

        for q, product in product_queries:
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            yield product, query_exprs
