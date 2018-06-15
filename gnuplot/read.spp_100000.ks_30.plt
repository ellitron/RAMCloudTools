set title "Latency vs. Value Size"
set xlabel "Value Size (B)"
set ylabel "Latency (us)"
set logscale xy
set key autotitle columnhead
set terminal postscript eps color
set output "read.spp_100000.ks_30.eps"
plot for [i=2:12] "read.spp_100000.ks_30.dat" u 1:i with lines lc i
