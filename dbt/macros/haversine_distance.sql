{% macro haversine_distance(lat1, lon1, lat2, lon2) %}

    {% set earth_radius_km = 6371 %}

    {{ return(
        earth_radius_km ~ ' * 2 * ASIN(SQRT(' ~
        'POWER(SIN(RADIANS(' ~ lat2 ~ ' - ' ~ lat1 ~ ') / 2), 2) + ' ~
        'COS(RADIANS(' ~ lat1 ~ ')) * COS(RADIANS(' ~ lat2 ~ ')) * ' ~
        'POWER(SIN(RADIANS(' ~ lon2 ~ ' - ' ~ lon1 ~ ') / 2), 2)' ~
        '))'
    ) }}

{% endmacro %}