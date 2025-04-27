import coflux as cf
import requests
import cv2
import skimage as sk
import numpy as np
from pathlib import Path


@cf.task(memo=True)
def fetch_photo(i: int = 0):
    r = requests.get("https://picsum.photos/1000")
    r.raise_for_status()
    path = Path("photo.jpg")
    path.write_bytes(r.content)
    return cf.persist_asset(path)


def _restore_image(image_f: cf.Execution[cf.Asset]):
    path = cf.restore_asset(image_f.result())
    return cv2.imread(str(path))


def _persist_image(img):
    cv2.imwrite("result.jpg", img)
    return cf.persist_asset("result.jpg")


@cf.task(wait=True)
def to_grayscale(image_f: cf.Execution[cf.Asset]):
    img = _restore_image(image_f)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return _persist_image(img)


@cf.task(wait=True)
def apply_blur(image_f: cf.Execution[cf.Asset]):
    img = _restore_image(image_f)
    img = cv2.GaussianBlur(img, (5, 5), 0)
    return _persist_image(img)


@cf.task(wait={"image_f"})
def detect_edges(image_f: cf.Execution[cf.Asset], kernel_size=5, threshold=128):
    img = _restore_image(image_f)
    cf.log_info("Input: {shape}", shape=img.shape, nonzeros=np.count_nonzero(img))
    img = cv2.Canny(img, 50, 150)
    cf.log_info("Canny: {shape}", shape=img.shape, nonzeros=np.count_nonzero(img))
    edges = sk.morphology.closing(img, sk.morphology.square(kernel_size))
    cf.log_info("Closing: {shape}", shape=img.shape, nonzeros=np.count_nonzero(img))
    return edges


@cf.task(wait=True)
def detect_contours(edges_f: cf.Execution, image_f: cf.Execution[cf.Asset]):
    img = _restore_image(image_f)
    edges = edges_f.result()
    cf.log_info("Edges: {shape}", shape=edges.shape)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cv2.drawContours(img, contours, -1, (0, 255, 0), 2)
    return _persist_image(img)


@cf.workflow()
def pipeline():
    original_f = fetch_photo.submit()
    grayscale_f = to_grayscale.submit(original_f)
    blurred_f = apply_blur.submit(grayscale_f)
    edges_f = detect_edges.submit(blurred_f)
    return detect_contours.submit(edges_f, original_f)
