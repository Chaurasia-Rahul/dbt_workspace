-- models/generate_models.sql

{% set start_year = 2011 %}
{% set end_year = 2014 %}  {# Adjust the end year as needed #}

{% for year in range(start_year, end_year + 1) %}
    {{ generate_model(year) }}
{% endfor %}
