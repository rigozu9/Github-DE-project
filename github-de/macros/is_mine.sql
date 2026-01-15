{% macro is_mine(email_expr, mine_email='rikunummi9@gmail.com') %}
coalesce(trim({{ email_expr }}) ilike '{{ mine_email }}', false)
{% endmacro %}
