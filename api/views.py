from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import HttpResponse
from rest_framework import status
from django.db import transaction, connection
from django.http import JsonResponse
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from .models import (
    AccInvMast, AccInvDetails, AccProduct, AccPurchaseMaster, 
    AccPurchaseDetails, AccProduction, AccProductionDetails, AccUsers
)
from .serializers import (
    AccInvMastSerializer, AccInvDetailsSerializer, AccProductSerializer,
    AccPurchaseMasterSerializer, AccPurchaseDetailsSerializer,
    AccProductionSerializer, AccProductionDetailsSerializer, AccUsersSerializer
)

# Setup logging
logger = logging.getLogger(__name__)

# Map table names to models and serializers
TABLE_MAPPING = {
        'acc_users': {
        'model': AccUsers,
        'serializer': AccUsersSerializer,
        'required_fields': ['id', 'pass_field'],
        'field_processors': {
            'id': lambda x: str(x).strip() if x is not None else None,
            'pass_field': lambda x: str(x).strip() if x is not None else None,
            'role': lambda x: str(x).strip() if x is not None else None
        }
    },
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
            'billedcost': lambda x: Decimal(str(x)) if x is not None else None,
            'basicprice': lambda x: Decimal(str(x)) if x is not None else None,
            'partqty': lambda x: Decimal(str(x)) if x is not None else None
        }
    },
    'acc_purchasemaster': {
        'model': AccPurchaseMaster,
        'serializer': AccPurchaseMasterSerializer,
        'required_fields': ['slno'],
        'field_processors': {
            'slno': lambda x: int(float(x)) if x is not None else None,
            'date': lambda x: datetime.strptime(x, '%Y-%m-%d').date() if isinstance(x, str) and x else x,
            'pdate': lambda x: datetime.strptime(x, '%Y-%m-%d').date() if isinstance(x, str) and x else x
        }
    },
    'acc_purchasedetails': {
        'model': AccPurchaseDetails,
        'serializer': AccPurchaseDetailsSerializer,
        'required_fields': ['billno', 'code'],
        'field_processors': {
            'billno': lambda x: int(float(x)) if x is not None else None,
            'quantity': lambda x: Decimal(str(x)) if x is not None else None
        }
    },
    'acc_production': {
        'model': AccProduction,
        'serializer': AccProductionSerializer,
        'required_fields': ['productionno'],
        'field_processors': {
            'date': lambda x: datetime.strptime(x, '%Y-%m-%d').date() if isinstance(x, str) and x else x
        }
    },
    'acc_productiondetails': {
        'model': AccProductionDetails,
        'serializer': AccProductionDetailsSerializer,
        'required_fields': ['masterno', 'code'],
        'field_processors': {
            'qty': lambda x: Decimal(str(x)) if x is not None else None
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


# Dictionary to track which tables have been truncated in the current sync session
truncated_tables = {}

@api_view(['POST'])
def sync_data(request):
    """
    Modified sync endpoint that only truncates table on the FIRST batch, 
    then appends subsequent batches.
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
        is_first_batch = request.data.get('is_first_batch', True)  
        is_last_batch = request.data.get('is_last_batch', True)  

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
            f"Starting sync for table: {table_name}, records: {len(data)}, first_batch: {is_first_batch}")
        start_time = datetime.now()

        # Skip validation for empty data
        if not data:
            if is_first_batch:
                with transaction.atomic():
                    deleted_count = truncate_table_fast(Model)
                    truncated_tables[table_name] = True

                return Response({
                    'success': True,
                    'message': f'Successfully cleared {table_name}',
                    'table': table_name,
                    'records_processed': 0,
                    'records_deleted': deleted_count,
                    'records_inserted': 0,
                    'processing_time_seconds': (datetime.now() - start_time).total_seconds()
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    'success': True,
                    'message': f'No data to process for {table_name}',
                    'table': table_name,
                    'records_processed': 0,
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
                'validation_errors': validation_errors[:5],
                'total_errors': len(validation_errors),
                'sample_data': data[:2] if data else []
            }, status=status.HTTP_400_BAD_REQUEST)

        logger.info(
            f"Validation completed. Processing {len(validated_data)} valid records...")

        # Perform the operation in a transaction
        with transaction.atomic():
            deleted_count = 0
            
            # Only truncate on the first batch
            if is_first_batch:
                deleted_count = truncate_table_fast(Model)
                truncated_tables[table_name] = True
                logger.info(f"Truncated table {table_name} (first batch)")
            else:
                logger.info(f"Appending to table {table_name} (subsequent batch)")

            # Insert data
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
            'records_deleted': deleted_count if is_first_batch else 0,
            'records_inserted': inserted_count,
            'validation_errors': len(validation_errors),
            'processing_time_seconds': round(processing_time, 2),
            'records_per_second': round(len(validated_data) / processing_time, 2) if processing_time > 0 else 0,
            'is_first_batch': is_first_batch,
            'is_last_batch': is_last_batch
        }

        logger.info(f"Sync completed for {table_name}: {response_data}")
        return Response(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        return Response({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Add a new endpoint to reset truncation tracking
@api_view(['POST'])
def reset_sync_session(request):
    """
    Reset the sync session - clears truncation tracking
    """
    global truncated_tables
    truncated_tables.clear()
    logger.info("Sync session reset - truncation tracking cleared")
    
    return Response({
        'success': True,
        'message': 'Sync session reset successfully'
    }, status=status.HTTP_200_OK)


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


# Home URL
def home(request):
    return HttpResponse("Welcome to the OMEGA Sync API 🚀")



@api_view(['GET'])
def get_table_info(request, table_name):
    """
    Get detailed information about a specific table
    """
    table_name = table_name.lower()
    
    if table_name not in TABLE_MAPPING:
        return Response({
            'success': False,
            'error': f'Table {table_name} not found. Available tables: {list(TABLE_MAPPING.keys())}'
        }, status=status.HTTP_404_NOT_FOUND)
    
    try:
        Model = TABLE_MAPPING[table_name]['model']
        record_count = Model.objects.count()
        
        # Get field information
        fields = []
        for field in Model._meta.get_fields():
            fields.append({
                'name': field.name,
                'type': field.__class__.__name__,
                'null': getattr(field, 'null', False),
                'blank': getattr(field, 'blank', False),
                'primary_key': getattr(field, 'primary_key', False)
            })
        
        return Response({
            'success': True,
            'table_name': table_name,
            'model_name': Model.__name__,
            'record_count': record_count,
            'fields': fields,
            'required_fields': TABLE_MAPPING[table_name]['required_fields']
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': f'Failed to get table info: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['DELETE'])
def clear_table(request, table_name):
    """
    Clear all data from a specific table
    """
    table_name = table_name.lower()
    
    if table_name not in TABLE_MAPPING:
        return Response({
            'success': False,
            'error': f'Table {table_name} not found'
        }, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        Model = TABLE_MAPPING[table_name]['model']
        
        with transaction.atomic():
            deleted_count = truncate_table_fast(Model)
        
        return Response({
            'success': True,
            'message': f'Successfully cleared table {table_name}',
            'records_deleted': deleted_count
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': f'Failed to clear table: {str(e)}'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)