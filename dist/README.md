# README
## Generated CSVs

These data sets are generated for the `dev` variant, to be used for the BI workload.

If you are looking for data sets to implement the Interactive workload, please consult the `stable` branch or reach out to us.

{% for file in site.static_files %}
  {% if file.extname == ".csv" -%}
    * [{{ file.path }}]({{ site.baseurl }}{{ file.path }})
  {%- endif %}
{% endfor %}
