from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^search_results/$', views.search, name='search'),
    url(r'^adv_search_results/$', views.adv_search, name='adv_search'),
    url(r'^faceted_search_results/$', views.faceted_search, name='faceted_search'),
    url(r'^wordcloud/$', views.wordcloud, name='wordcloud'),
    url(r'^timeline/$', views.timeline, name='timeline'),
    url(r'^(?P<motie_id>[0-9A-Za-z]+)/$', views.detail, name='detail')
]
