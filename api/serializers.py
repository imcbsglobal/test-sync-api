from rest_framework import serializers
from .models import AccInvMast, AccInvDetails, AccProduct, AccPurchaseMaster, AccPurchaseDetails, AccProduction, AccProductionDetails, AccUsers
from decimal import Decimal


class AccUsersSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccUsers
        fields = ['id', 'pass_field', 'role']

class AccInvMastSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccInvMast
        fields = '__all__'

    def to_internal_value(self, data):
        # Force slno to be a proper int if it's a float
        slno = data.get("slno")
        if slno is not None:
            try:
                data["slno"] = int(float(slno))
            except (ValueError, TypeError):
                pass
        return super().to_internal_value(data)


class AccInvDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccInvDetails
        fields = '__all__'

    def to_internal_value(self, data):
        invno = data.get("invno")
        if invno is not None:
            try:
                data["invno"] = int(float(invno))
            except (ValueError, TypeError):
                pass
        return super().to_internal_value(data)


class AccProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccProduct
        fields = '__all__'


class AccPurchaseMasterSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccPurchaseMaster
        fields = ['slno', 'date', 'pdate']

    def to_internal_value(self, data):
        slno = data.get("slno")
        if slno is not None:
            try:
                data["slno"] = int(float(slno))
            except (ValueError, TypeError):
                raise serializers.ValidationError({"slno": "Invalid value"})
        return super().to_internal_value(data)


class AccPurchaseDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccPurchaseDetails
        fields = ['billno', 'code', 'quantity']

    def to_internal_value(self, data):
        # FIX: Convert billno to int since the model field is DecimalField
        billno = data.get("billno")
        if billno is not None:
            try:
                data["billno"] = int(float(billno))
            except (ValueError, TypeError):
                raise serializers.ValidationError({"billno": "Invalid billno value"})
        return super().to_internal_value(data)


class AccProductionSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccProduction
        fields = ['productionno', 'date']

    def to_internal_value(self, data):
        prodno = data.get("productionno")
        if prodno is not None:
            try:
                data["productionno"] = int(float(prodno))
            except (ValueError, TypeError):
                raise serializers.ValidationError({"productionno": "Invalid value"})
        return super().to_internal_value(data)


class AccProductionDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = AccProductionDetails
        fields = ['masterno', 'code', 'qty']