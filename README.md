# RoadMap_DE

Lộ trình tự học Data Engineer cho người đi từ Software Engineer. Nội dung viết bằng LaTeX, gồm 14 module, từ M0 đến M13. Các lab chạy code trong repo [lakehouse](https://github.com/DPhO05/lakehouse), nên clone repo đó trước khi làm M0.

## Biên dịch

Đã thử với [tectonic](https://tectonic-typesetting.github.io/) 0.17.0:

```bash
mkdir -p build && tectonic -o build lo_trinh_data_engineer.tex
```

PDF nằm trong `build/`.

## Cấu trúc

- `lo_trinh_data_engineer.tex`: file chính, nạp các module bằng `\input`.
- `modules/`: nội dung từng module.
- `solutions/`: lời giải mẫu của các lab, chia theo module.
