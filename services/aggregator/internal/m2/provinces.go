package m2

// BNPB's versioned static reference for this PoC: representative provincial
// centres, not administrative polygons. See docs/m2-problems-4-5.md.
var provinces = []struct {
	name     string
	lat, lon float64
}{
	{"Aceh", 4.6951, 96.7494}, {"Sumatera Utara", 2.1154, 99.5451}, {"Sumatera Barat", -0.7399, 100.8000},
	{"Riau", 0.2933, 101.7068}, {"Kepulauan Riau", 3.9457, 108.1429}, {"Jambi", -1.6101, 103.6131},
	{"Sumatera Selatan", -3.3194, 103.9144}, {"Bengkulu", -3.5778, 102.3464}, {"Lampung", -4.5586, 105.4068},
	{"Kepulauan Bangka Belitung", -2.7411, 106.4406}, {"DKI Jakarta", -6.2088, 106.8456}, {"Banten", -6.4058, 106.0640},
	{"Jawa Barat", -7.0909, 107.6689}, {"Jawa Tengah", -7.1510, 110.1403}, {"DI Yogyakarta", -7.8754, 110.4262},
	{"Jawa Timur", -7.5361, 112.2384}, {"Bali", -8.4095, 115.1889}, {"Nusa Tenggara Barat", -8.6529, 117.3616},
	{"Nusa Tenggara Timur", -8.6574, 121.0794}, {"Kalimantan Barat", -0.2788, 111.4753}, {"Kalimantan Tengah", -1.6815, 113.3824},
	{"Kalimantan Selatan", -3.0926, 115.2838}, {"Kalimantan Timur", 0.5387, 116.4194}, {"Kalimantan Utara", 3.0731, 116.0414},
	{"Sulawesi Utara", 0.6247, 123.9750}, {"Gorontalo", 0.6999, 122.4467}, {"Sulawesi Tengah", -1.4300, 121.4456},
	{"Sulawesi Barat", -2.8441, 119.2321}, {"Sulawesi Selatan", -3.6688, 119.9741}, {"Sulawesi Tenggara", -4.1449, 122.1746},
	{"Maluku", -3.2385, 130.1453}, {"Maluku Utara", 1.5709, 127.8088}, {"Papua Barat", -1.3361, 133.1747},
	{"Papua Barat Daya", -1.0000, 131.5000}, {"Papua", -2.5000, 139.5000}, {"Papua Tengah", -4.0000, 136.0000},
	{"Papua Pegunungan", -4.0000, 139.0000}, {"Papua Selatan", -6.0000, 139.5000},
}

func nearestProvince(lat, lon float64) string {
	best := 1e20
	name := ""
	for _, p := range provinces {
		d := distance(lat, lon, p.lat, p.lon)
		if d < best {
			best = d
			name = p.name
		}
	}
	return name
}
