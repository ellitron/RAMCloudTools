set title "Latency vs. Value Size"
set xlabel "Value Size (B)"
set ylabel "Latency (us)"
set logscale xy
set key autotitle columnhead
set terminal postscript eps color
set output "write.spp_100000.rf_3.ks_30.eps"
plot for [i=2:12] "write.spp_100000.rf_3.ks_30.csv" u 1:i with lines lc i
