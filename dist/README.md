# LDBC SNB Datagen (Spark variant) – Latest artefacts

This README is deployed to <https://ldbcouncil.org/ldbc_snb_datagen_spark>.

## Generated data sets

The following data sets were generated for the **LDBC Social Network Benchmark's BI (Business Intelligence) workload** by the latest commit at <https://github.com/ldbc/ldbc_snb_datagen_spark>.

If you are looking for data sets of the **SNB Interactive workload**, please use the [legacy Hadoop-based Datagen](https://github.com/ldbc/ldbc_snb_datagen_hadoop) or download them from the [SURF/CWI data repository](https://hdl.handle.net/11112/e6e00558-a2c3-9214-473e-04a16de09bf8).

{% for file in site.static_files %}
  {% if file.extname == ".zip" -%}
    * [{{ file.path | replace: "/", "" }}]({{ site.baseurl }}{{ file.path }})
  {%- endif %}
{% endfor %}
