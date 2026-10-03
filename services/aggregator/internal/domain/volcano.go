package domain

import (
	"strings"
)

type VolcanoRef struct {
	ID        string  `json:"volcano_id"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// static volcano reference
var defaultVolcanoes = map[string]VolcanoRef{
	"merapi": {
		ID:        "merapi",
		Name:      "Gunung Merapi",
		Latitude:  -7.5407,
		Longitude: 110.4457,
	},
	"sinabung": {
		ID:        "sinabung",
		Name:      "Gunung Sinabung",
		Latitude:  3.1700,
		Longitude: 98.3920,
	},
	"krakatau": {
		ID:        "krakatau",
		Name:      "Gunung Anak Krakatau",
		Latitude:  -6.1020,
		Longitude: 105.4230,
	},
	"semeru": {
		ID:        "semeru",
		Name:      "Gunung Semeru",
		Latitude:  -8.1080,
		Longitude: 112.9220,
	},
	"kelud": {
		ID:        "kelud",
		Name:      "Gunung Kelud",
		Latitude:  -7.9300,
		Longitude: 112.3080,
	},
	"agung": {
		ID:        "agung",
		Name:      "Gunung Agung",
		Latitude:  -8.3430,
		Longitude: 115.5080,
	},
	"bromo": {
		ID:        "bromo",
		Name:      "Gunung Bromo",
		Latitude:  -7.9425,
		Longitude: 112.9530,
	},
	"marapi": {
		ID:        "marapi",
		Name:      "Gunung Marapi",
		Latitude:  -0.3810,
		Longitude: 100.4730,
	},
	"soputan": {
		ID:        "soputan",
		Name:      "Gunung Soputan",
		Latitude:  1.1120,
		Longitude: 124.7370,
	},
	"anak-krakatau": {
		ID:        "anak-krakatau",
		Name:      "Gunung Anak Krakatau",
		Latitude:  -6.10,
		Longitude: 105.42,
	},
	"lewotobi-laki-laki": {
		ID:        "lewotobi-laki-laki",
		Name:      "Gunung Lewotobi Laki-laki",
		Latitude:  -8.54,
		Longitude: 122.78,
	},
	"tambora": {
		ID:        "tambora",
		Name:      "Gunung Tambora",
		Latitude:  -8.2500,
		Longitude: 118.0000,
	},
}

func LookupVolcano(volcanoID string) (VolcanoRef, bool) {
	key := strings.ToLower(strings.TrimSpace(volcanoID))
	key = strings.TrimPrefix(key, "v-")
	v, ok := defaultVolcanoes[key]
	return v, ok
}

func GetAllVolcanoes() []VolcanoRef {
	list := make([]VolcanoRef, 0, len(defaultVolcanoes))
	for _, v := range defaultVolcanoes {
		list = append(list, v)
	}
	return list
}
