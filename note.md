This repository is a **React frontend**; there is **no backend server implementation** in the workspace. The list below is every **API path the app calls**, relative to `baseURL` from `REACT_APP_API_URL_DEV` / `REACT_APP_API_URL_PROD` (see `src/utils/StaticVariables.js`).

### Auth & user
| Method | Path |
|--------|------|
| POST | `login` |
| POST | `otp/send-otp` |
| POST | `otp/verify-otp` |
| POST | `contact` |
| GET | `user_info` |
| PUT | `update-password` |
| PUT | `users/reset_password` |
| GET | `auth/logs` |

`ContentNav.js` calls `axios.post(baseURL, "logout", …)`, which is almost certainly a bug; the intended path is likely **`logout`** on the same base (POST).

### Streams / sources
| Method | Path |
|--------|------|
| GET | `source/user` |
| POST | `source` |
| PUT | `source` |
| DELETE | `source` |
| POST | `source/bulk-upload-with-location` |
| GET | `streams/locations/search` (query string built in code) |
| POST | `stop_stream/{id}` |
| GET | `param_stream/user` |
| PUT | `param_stream/user` |

### Notifications (HTTP)
| Method | Path |
|--------|------|
| GET | `notify` |

### Search / workspace
| Method | Path |
|--------|------|
| GET | `locations/hierarchy` |
| GET | `workspace/search_results_with_location` |

### Location hierarchy (filters)
| Method | Path |
|--------|------|
| GET | `locations/list` |
| GET | `areas/list` |
| GET | `buildings/list` |
| GET | `floor-levels/list` |
| GET | `zones/list` |

### Analytics
| Method | Path |
|--------|------|
| GET | `analytics/fire/threshold-violations-by-camera` |
| GET | `analytics/fire/detections-by-camera` |
| GET | `analytics/cameras/frame-counts` |
| GET | `analytics/cameras/average-people` |
| GET | `analytics/cameras/average-gender` |
| GET | `analytics/cameras/unique` |
| GET | `analytics/cameras/frame-comparison` |
| GET | `analytics/zones/gender-by-weekday` |
| GET | `analytics/floors/people-by-weekday` |
| GET | `analytics/floors/people-by-hour` |
| GET | `analytics/floors/average-people` |
| GET | `analytics/busiest-hours/by-weekday` |

### Other
| Method | Path |
|--------|------|
| GET | `prediction_data` |

### Separate streaming base (`StreamURL` — `REACT_APP_API_STREAM_DEV` / `REACT_APP_API_STREAM_PROD`)
These are **not** under `baseURL`; the app appends:
- `notify?token=…` (WebSocket in `useNotificationWebSocket.js`)
- `stream?stream_id=…&token=…` (in `StreamCards.js`)

**Full URLs** are always: `{baseURL or StreamURL from env}` + the path/query above (your env values should end with `/` if the code assumes paths like `login` without a leading slash—check how `REACT_APP_API_URL_*` is set).