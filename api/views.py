from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import HttpResponse
from rest_framework import status
from django.db import transaction, connection
from django.http import JsonResponse
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .models import AccInvMast, AccInvDetails, AccProduct
from .serializers import AccInvMastSerializer, AccInvDetailsSerializer, AccProductSerializer

# Setup logging
logger = logging.getLogger(__name__)

# Map table names to models and serializers
TABLE_MAPPING = {
    'acc_invmast': {
        'model': AccInvMast,
        'serializer': AccInvMastSerializer,
        'required_fields': ['slno'],
        'field_processors': {
            'slno': lambda x: int(float(x)) if x is not None else None,
            'invdate': lambda x: datetime.strptime(x, '%Y-%m-%d').date() if isinstance(x, str) and x else x
        }
    },
    'acc_invdetails': {
        'model': AccInvDetails,
        'serializer': AccInvDetailsSerializer,
        'required_fields': ['invno', 'code'],
        'field_processors': {
            'invno': lambda x: int(float(x)) if x is not None else None,
            'quantity': lambda x: Decimal(str(x)) if x is not None else None
        }
    },
    'acc_product': {
        'model': AccProduct,
        'serializer': AccProductSerializer,
        'required_fields': ['code'],
        'field_processors': {
            'quantity': lambda x: Decimal(str(x)) if x is not None else None,
            'openingquantity': lambda x: Decimal(str(x)) if x is not None else None,
            'billedcost': lambda x: Decimal(str(x)) if x is not None else None
        }
    }
}


def fast_validate_and_process_data(data, table_name):
    """
    Fast validation and data processing without using serializers for bulk operations
    """
    if table_name not in TABLE_MAPPING:
        raise ValueError(f"Unsupported table: {table_name}")

    table_config = TABLE_MAPPING[table_name]
    required_fields = table_config.get('required_fields', [])
    field_processors = table_config.get('field_processors', {})

    processed_data = []
    errors = []

    for i, record in enumerate(data):
        try:
            # Check required fields
            for field in required_fields:
                if field not in record or record[field] is None or record[field] == '':
                    errors.append({
                        'record_index': i,
                        'error': f'Required field "{field}" is missing or empty',
                        'record': record
                    })
                    continue

            # Process fields
            processed_record = {}
            for key, value in record.items():
                if key in field_processors:
                    try:
                        processed_record[key] = field_processors[key](value)
                    except (ValueError, TypeError, InvalidOperation) as e:
                        errors.append({
                            'record_index': i,
                            'error': f'Field "{key}" processing failed: {str(e)}',
                            'record': record
                        })
                        break
                else:
                    processed_record[key] = value
            else:
                # Only add if no errors occurred in the inner loop
                processed_data.append(processed_record)

        except Exception as e:
            errors.append({
                'record_index': i,
                'error': f'Record processing failed: {str(e)}',
                'record': record
            })

    return processed_data, errors


def bulk_insert_optimized(Model, data, batch_size=5000):
    """
    Optimized bulk insert with larger batch sizes and better performance
    """
    total_inserted = 0

    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        instances = [Model(**item) for item in batch]

        # Use bulk_create with ignore_conflicts=False for better performance
        Model.objects.bulk_create(
            instances, batch_size=batch_size, ignore_conflicts=False)
        total_inserted += len(batch)

        if i % (batch_size * 2) == 0:  # Log every 2 batches
            logger.info(f"Inserted {total_inserted}/{len(data)} records")

    return total_inserted


def truncate_table_fast(Model):
    """
    Fast table truncation using raw SQL for better performance
    """
    table_name = Model._meta.db_table

    with connection.cursor() as cursor:
        # Use TRUNCATE for much faster deletion (if supported)
        try:
            cursor.execute(f'TRUNCATE TABLE {table_name}')
            logger.info(f"Fast truncated table {table_name}")
            return True
        except Exception as e:
            # Fallback to DELETE if TRUNCATE is not supported
            logger.warning(
                f"TRUNCATE failed for {table_name}, using DELETE: {str(e)}")
            cursor.execute(f'DELETE FROM {table_name}')
            deleted_count = cursor.rowcount
            logger.info(f"Deleted {deleted_count} records from {table_name}")
            return deleted_count


@api_view(['POST'])
def sync_data(request):
    """
    Optimized sync endpoint that clears existing data and inserts new data for a specific table.
    """
    try:
        # Validate request data
        if not request.data:
            return Response({
                'success': False,
                'error': 'No data provided'
            }, status=status.HTTP_400_BAD_REQUEST)

        database = request.data.get('database', 'OMEGA')
        table_name = request.data.get('table', '').lower()
        data = request.data.get('data', [])

        # Validate required fields
        if not table_name:
            return Response({
                'success': False,
                'error': 'Table name is required'
            }, status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(data, list):
            return Response({
                'success': False,
                'error': 'Data must be a list'
            }, status=status.HTTP_400_BAD_REQUEST)

        # Check if table is supported
        if table_name not in TABLE_MAPPING:
            return Response({
                'success': False,
                'error': f'Table {table_name} is not supported. Supported tables: {list(TABLE_MAPPING.keys())}'
            }, status=status.HTTP_400_BAD_REQUEST)

        # Get model
        Model = TABLE_MAPPING[table_name]['model']

        logger.info(
            f"Starting optimized sync for table: {table_name}, records: {len(data)}")
        start_time = datetime.now()

        # Skip validation for empty data
        if not data:
            with transaction.atomic():
                deleted_count = truncate_table_fast(Model)

            return Response({
                'success': True,
                'message': f'Successfully cleared {table_name}',
                'table': table_name,
                'records_processed': 0,
                'records_deleted': deleted_count,
                'records_inserted': 0,
                'processing_time_seconds': (datetime.now() - start_time).total_seconds()
            }, status=status.HTTP_200_OK)

        # Fast validation and processing
        logger.info("Starting fast validation and processing...")
        validated_data, validation_errors = fast_validate_and_process_data(
            data, table_name)

        # If there are validation errors, return them
        if validation_errors:
            logger.error(
                f"Validation failed for {table_name}: {len(validation_errors)} errors")
            return Response({
                'success': False,
                'error': 'Data validation failed',
                # Return first 5 errors
                'validation_errors': validation_errors[:5],
                'total_errors': len(validation_errors),
                'sample_data': data[:2] if data else []
            }, status=status.HTTP_400_BAD_REQUEST)

        logger.info(
            f"Validation completed. Processing {len(validated_data)} valid records...")

        # Perform the clear and insert operation in a transaction
        with transaction.atomic():
            # Step 1: Fast table truncation
            deleted_count = truncate_table_fast(Model)

            # Step 2: Optimized bulk insert
            if validated_data:
                inserted_count = bulk_insert_optimized(
                    Model, validated_data, batch_size=5000)
                logger.info(
                    f"Successfully inserted {inserted_count} records into {table_name}")
            else:
                inserted_count = 0

        # Calculate processing time
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        # Success response
        response_data = {
            'success': True,
            'message': f'Successfully synced {len(validated_data)} records to {table_name}',
            'table': table_name,
            'records_processed': len(data),
            'records_deleted': deleted_count if isinstance(deleted_count, int) else 'truncated',
            'records_inserted': inserted_count,
            'validation_errors': len(validation_errors),
            'processing_time_seconds': round(processing_time, 2),
            'records_per_second': round(len(validated_data) / processing_time, 2) if processing_time > 0 else 0
        }

        logger.info(f"Sync completed for {table_name}: {response_data}")
        return Response(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        return Response({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def sync_status(request):
    """
    Get the current status of all tables (record counts)
    """
    try:
        status_data = {}

        for table_name, model_info in TABLE_MAPPING.items():
            Model = model_info['model']
            count = Model.objects.count()
            status_data[table_name] = {
                'record_count': count,
                'model': Model.__name__
            }

        return Response({
            'success': True,
            'tables': status_data,
            'total_records': sum(table['record_count'] for table in status_data.values())
        }, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Status check failed: {str(e)}")
        return Response({
            'success': False,
            'error': f'Failed to get status: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def health_check(request):
    """
    Simple health check endpoint
    """
    return Response({
        'status': 'healthy',
        'message': 'Omega API is running'
    }, status=status.HTTP_200_OK)


# Additional optimization endpoint for very large datasets
@api_view(['POST'])
def sync_data_ultra_fast(request):
    """
    Ultra-fast sync for very large datasets (10k+ records) with minimal validation
    Use this only when data quality is guaranteed at the source
    """
    try:
        database = request.data.get('database', 'OMEGA')
        table_name = request.data.get('table', '').lower()
        data = request.data.get('data', [])

        if not table_name or table_name not in TABLE_MAPPING:
            return Response({
                'success': False,
                'error': 'Invalid table name'
            }, status=status.HTTP_400_BAD_REQUEST)

        Model = TABLE_MAPPING[table_name]['model']

        logger.info(
            f"Starting ULTRA-FAST sync for table: {table_name}, records: {len(data)}")
        start_time = datetime.now()

        with transaction.atomic():
            # Ultra-fast truncate
            truncate_table_fast(Model)

            # Ultra-fast bulk insert with larger batches
            if data:
                batch_size = 10000  # Much larger batches
                total_inserted = 0

                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    instances = [Model(**item) for item in batch]
                    Model.objects.bulk_create(instances, batch_size=batch_size)
                    total_inserted += len(batch)

                    if i % (batch_size * 5) == 0:  # Log every 5 batches
                        logger.info(
                            f"Ultra-fast inserted {total_inserted}/{len(data)} records")

        processing_time = (datetime.now() - start_time).total_seconds()

        return Response({
            'success': True,
            'message': f'Ultra-fast sync completed for {table_name}',
            'table': table_name,
            'records_processed': len(data),
            'records_inserted': len(data),
            'processing_time_seconds': round(processing_time, 2),
            'records_per_second': round(len(data) / processing_time, 2) if processing_time > 0 else 0
        }, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Ultra-fast sync failed: {str(e)}")
        return Response({
            'success': False,
            'error': f'Ultra-fast sync failed: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Home URL


def home(request):
    return HttpResponse("Welcome to the OMEGA Sync API 🚀")
