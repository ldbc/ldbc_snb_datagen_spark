# README

## Generated CSVs

{% for file in site.static_files %}
  {% if file.extname == ".csv" -%}
    * [{{ file.path }}]({{ site.baseurl }}{{ file.path }})
  {%- endif %}
{% endfor %}
