(function () {
    'use strict';

    let currentStep = 0;
    const totalSteps = 4;

    let map = null;
    let reviewMap = null;
    let marker = null;
    let reviewMarker = null;
    let radiusCircle = null;
    let selectedLat = null;
    let selectedLon = null;
    let locationName = '';

    const state = {
        primary: [],
        secondary: [],
        types: [],
        rejects: [],
    };

    // Step navigation

    window.nextStep = function () {
        if (!validateStep(currentStep)) return;
        if (currentStep < totalSteps - 1) {
            setStep(currentStep + 1);
        }
    };

    window.prevStep = function () {
        if (currentStep > 0) {
            setStep(currentStep - 1);
        }
    };

    function setStep(step) {
        document.querySelectorAll('.wizard-step').forEach(function (el) {
            el.classList.remove('active');
        });
        document.getElementById('step-' + step).classList.add('active');

        document.querySelectorAll('.step-dot').forEach(function (dot, i) {
            dot.classList.remove('active', 'completed');
            if (i < step) dot.classList.add('completed');
            if (i === step) dot.classList.add('active');
        });

        currentStep = step;
        window.scrollTo({ top: 0, behavior: 'smooth' });

        if (step === 2) {
            setTimeout(initMap, 100);
        }
        if (step === 3) {
            setTimeout(function () {
                initReviewMap();
                renderReview();
            }, 100);
        }
    }

    function validateStep(step) {
        if (step === 0) {
            var gk = document.getElementById('google-maps-key').value.trim();
            var ak = document.getElementById('gemini-key').value.trim();
            if (!gk) {
                shakeInput('google-maps-key');
                return false;
            }
            if (!ak) {
                shakeInput('gemini-key');
                return false;
            }
            return true;
        }
        if (step === 1) {
            if (state.primary.length === 0) {
                showError('generate-error', 'Generate or manually add at least one primary search query.');
                return false;
            }
            hideError('generate-error');
            return true;
        }
        if (step === 2) {
            if (selectedLat === null || selectedLon === null) {
                alert('Please select a location on the map, search for a place, or enter coordinates.');
                return false;
            }
            return true;
        }
        return true;
    }

    function shakeInput(id) {
        var el = document.getElementById(id);
        el.style.borderColor = 'var(--error)';
        el.focus();
        setTimeout(function () {
            el.style.borderColor = '';
        }, 2000);
    }

    function showError(id, msg) {
        var el = document.getElementById(id);
        el.textContent = msg;
        el.classList.remove('hidden');
    }

    function hideError(id) {
        document.getElementById(id).classList.add('hidden');
    }

    // Show/hide keys toggle
    document.getElementById('show-keys').addEventListener('change', function () {
        var type = this.checked ? 'text' : 'password';
        document.getElementById('google-maps-key').type = type;
        document.getElementById('gemini-key').type = type;
    });

    // Gemini search term generation

    window.generateSearchTerms = function () {
        var desc = document.getElementById('search-description').value.trim();
        if (!desc) {
            shakeInput('search-description');
            return;
        }

        var geminiKey = document.getElementById('gemini-key').value.trim();
        document.getElementById('generate-spinner').classList.remove('hidden');
        document.getElementById('generate-btn').disabled = true;
        hideError('generate-error');

        fetch('/api/generate-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ description: desc, gemini_api_key: geminiKey }),
        })
            .then(function (r) { return r.json(); })
            .then(function (data) {
                document.getElementById('generate-spinner').classList.add('hidden');
                document.getElementById('generate-btn').disabled = false;

                if (data.error) {
                    showError('generate-error', data.error);
                    return;
                }

                state.primary = data.primary_queries || [];
                state.secondary = data.secondary_queries || [];
                state.types = data.type_filters || [];
                state.rejects = data.reject_substrings || [];

                if (data.min_reviews !== undefined) {
                    document.getElementById('min-reviews').value = data.min_reviews;
                }

                renderTags();
                document.getElementById('generated-terms').classList.remove('hidden');
            })
            .catch(function (err) {
                document.getElementById('generate-spinner').classList.add('hidden');
                document.getElementById('generate-btn').disabled = false;
                showError('generate-error', 'Failed to connect to backend: ' + err.message);
            });
    };

    // Tag management

    function renderTags() {
        renderTagGroup('primary-queries', state.primary, 'primary');
        renderTagGroup('secondary-queries', state.secondary, 'secondary');
        renderTagGroup('type-filters', state.types, 'types');
        renderTagGroup('reject-substrings', state.rejects, 'rejects');
    }

    function renderTagGroup(containerId, items, field) {
        var container = document.getElementById(containerId);
        container.innerHTML = '';
        items.forEach(function (item, idx) {
            var tag = document.createElement('span');
            tag.className = 'tag';
            tag.textContent = item + ' ';
            var removeBtn = document.createElement('span');
            removeBtn.className = 'tag-remove';
            removeBtn.textContent = '×';
            removeBtn.addEventListener('click', function () {
                window.removeTag(field, idx);
            });
            tag.appendChild(removeBtn);
            container.appendChild(tag);
        });
    }

    window.addTag = function (field) {
        var input = document.getElementById('add-' + field);
        var val = input.value.trim();
        if (!val) return;
        state[field].push(val);
        input.value = '';
        renderTags();

        if (document.getElementById('generated-terms').classList.contains('hidden')) {
            document.getElementById('generated-terms').classList.remove('hidden');
        }
    };

    window.removeTag = function (field, idx) {
        state[field].splice(idx, 1);
        renderTags();
    };

    // Enter key support for tag inputs
    document.querySelectorAll('.tag-input').forEach(function (input) {
        input.addEventListener('keydown', function (e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                var field = this.id.replace('add-', '');
                window.addTag(field);
            }
        });
    });

    // Location: Map

    function initMap() {
        if (map) {
            map.invalidateSize();
            return;
        }

        map = L.map('map', { zoomControl: true }).setView([52.0, 19.0], 6);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap',
            maxZoom: 19,
        }).addTo(map);

        map.on('click', function (e) {
            setLocation(e.latlng.lat, e.latlng.lng, '');
        });
    }

    function initReviewMap() {
        if (reviewMap) {
            reviewMap.invalidateSize();
            updateReviewMap();
            return;
        }

        var center = (selectedLat !== null) ? [selectedLat, selectedLon] : [52.0, 19.0];
        var zoom = (selectedLat !== null) ? 11 : 6;

        reviewMap = L.map('review-map', { zoomControl: true, dragging: false, scrollWheelZoom: false }).setView(center, zoom);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap',
            maxZoom: 19,
        }).addTo(reviewMap);

        updateReviewMap();
    }

    function updateReviewMap() {
        if (!reviewMap || selectedLat === null) return;

        if (reviewMarker) reviewMap.removeLayer(reviewMarker);
        if (radiusCircle) reviewMap.removeLayer(radiusCircle);

        reviewMarker = L.marker([selectedLat, selectedLon]).addTo(reviewMap);

        var distKm = parseInt(document.getElementById('max-distance').value);
        radiusCircle = L.circle([selectedLat, selectedLon], {
            radius: distKm * 1000,
            color: '#6366f1',
            fillColor: '#6366f1',
            fillOpacity: 0.1,
            weight: 2,
        }).addTo(reviewMap);

        reviewMap.fitBounds(radiusCircle.getBounds().pad(0.1));
    }

    function setLocation(lat, lon, name) {
        selectedLat = parseFloat(lat.toFixed(6));
        selectedLon = parseFloat(lon.toFixed(6));
        locationName = name || (selectedLat + ', ' + selectedLon);

        if (map) {
            if (marker) map.removeLayer(marker);
            marker = L.marker([selectedLat, selectedLon]).addTo(map);
            map.setView([selectedLat, selectedLon], 12);
        }

        var display = document.getElementById('location-display');
        display.textContent = locationName + ' (' + selectedLat + ', ' + selectedLon + ')';
        document.getElementById('selected-location').classList.remove('hidden');
    }

    window.setLocationMode = function (mode) {
        document.querySelectorAll('.mode-btn').forEach(function (btn) {
            btn.classList.toggle('active', btn.dataset.mode === mode);
        });

        var areas = { search: 'search-input-area', manual: 'manual-input-area' };
        Object.keys(areas).forEach(function (key) {
            var el = document.getElementById(areas[key]);
            el.classList.toggle('hidden', key !== mode);
        });
    };

    // Location: Search

    window.searchPlace = function () {
        var query = document.getElementById('place-search').value.trim();
        if (!query) return;

        var apiKey = document.getElementById('google-maps-key').value.trim();
        fetch('/api/geocode?q=' + encodeURIComponent(query) + '&key=' + encodeURIComponent(apiKey))
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (data.error) {
                    alert(data.error);
                    return;
                }
                var container = document.getElementById('search-results');
                container.innerHTML = '';
                container.classList.remove('hidden');

                if (data.results && data.results.length > 0) {
                    data.results.forEach(function (result) {
                        var div = document.createElement('div');
                        div.className = 'search-result-item';
                        div.textContent = result.name;
                        div.addEventListener('click', function () {
                            setLocation(result.lat, result.lon, result.name);
                            container.classList.add('hidden');
                        });
                        container.appendChild(div);
                    });
                } else {
                    container.innerHTML = '<div class="search-result-item">No results found</div>';
                }
            })
            .catch(function () {
                alert('Failed to search. Check your API key and try again.');
            });
    };

    document.getElementById('place-search').addEventListener('keydown', function (e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            window.searchPlace();
        }
    });

    // Location: Manual coordinates

    window.applyManualCoords = function () {
        var lat = parseFloat(document.getElementById('manual-lat').value);
        var lon = parseFloat(document.getElementById('manual-lon').value);
        if (isNaN(lat) || isNaN(lon)) {
            alert('Enter valid latitude and longitude values.');
            return;
        }
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            alert('Coordinates out of range.');
            return;
        }
        setLocation(lat, lon, '');
        if (!map) {
            setTimeout(function () {
                initMap();
                setLocation(lat, lon, '');
            }, 200);
        }
    };

    // Distance slider

    window.updateDistance = function (val) {
        document.getElementById('distance-value').textContent = val;
        updateReviewMap();
        renderReview();
    };

    // Review

    function renderReview() {
        var desc = document.getElementById('search-description').value.trim() || '(not set)';
        var dist = document.getElementById('max-distance').value;
        var loc = locationName || '(not set)';
        var coords = (selectedLat !== null) ? selectedLat + ', ' + selectedLon : '(not set)';

        var html = '<h3>Configuration Summary</h3>';
        html += reviewItem('Search For', desc);
        html += reviewItem('Location', loc);
        html += reviewItem('Coordinates', coords);
        html += reviewItem('Max Distance', dist + ' km');
        html += reviewItem('Primary Queries', state.primary.join(', ') || '(none)');
        html += reviewItem('Secondary Queries', state.secondary.join(', ') || '(none)');
        html += reviewItem('Type Filters', state.types.join(', ') || '(none)');
        html += reviewItem('Reject Substrings', state.rejects.join(', ') || '(none)');
        html += reviewItem('Min Reviews', document.getElementById('min-reviews').value);

        document.getElementById('review-summary').innerHTML = html;
    }

    function reviewItem(label, value) {
        return '<div class="review-item"><span class="review-label">' + label + '</span><span class="review-value">' + escapeHtml(value) + '</span></div>';
    }

    function escapeHtml(text) {
        var div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Save config

    window.saveConfig = function () {
        var saveBtn = document.querySelector('.btn-finish');
        saveBtn.disabled = true;
        saveBtn.textContent = 'Saving…';

        var config = {
            description: document.getElementById('search-description').value.trim(),
            center: {
                lat: selectedLat,
                lon: selectedLon,
                name: locationName,
            },
            max_distance_km: parseInt(document.getElementById('max-distance').value),
            queries: {
                primary: state.primary,
                secondary: state.secondary,
            },
            type_filters: state.types,
            domain_reject_substrings: state.rejects,
            min_reviews: parseInt(document.getElementById('min-reviews').value) || 50,
        };

        var envData = {
            GOOGLE_MAPS_API_KEY: document.getElementById('google-maps-key').value.trim(),
            GEMINI_API_KEY: document.getElementById('gemini-key').value.trim(),
        };

        var statusEl = document.getElementById('save-status');

        fetch('/api/save-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ config: config, env: envData }),
        })
            .then(function (r) { return r.json(); })
            .then(function (data) {
                saveBtn.disabled = false;
                saveBtn.textContent = 'Generate Config';
                statusEl.classList.remove('hidden', 'error');
                if (data.error) {
                    statusEl.className = 'save-status error';
                    statusEl.textContent = '✗ ' + data.error;
                } else {
                    statusEl.className = 'save-status success';
                    statusEl.innerHTML = '✓ Configuration saved!<br><small>Files written: search_config.json, .env</small><br><small>Run <code>python run.py --radius-scan --center-lat ' + selectedLat + ' --center-lon ' + selectedLon + ' --radius-km ' + config.max_distance_km + '</code> to start.</small>';
                }
            })
            .catch(function (err) {
                saveBtn.disabled = false;
                saveBtn.textContent = 'Generate Config';
                statusEl.classList.remove('hidden');
                statusEl.className = 'save-status error';
                statusEl.textContent = '✗ Failed to save: ' + err.message;
            });
    };

})();
