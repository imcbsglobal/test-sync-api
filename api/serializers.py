from rest_framework import serializers
from .models import AccInvMast, AccInvDetails, AccProduct
from decimal import Decimal


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
