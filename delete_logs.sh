rm -f backend/by_year_and_station_trips_count/.storage/*
rm -f backend/stations_distance_running_avg/.storage/*
rm -f backend/stations_manager/.storage/*
rm -f backend/trip_duration_running_avg/.storage/*
rm -f backend/weather_manager/.storage/*
find . -type d -name '.eof*' -exec rm -rf {} \;
find . -type d -name '.clients*' -exec rm -rf {} \;