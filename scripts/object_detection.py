import pandas as pd
import torch
import cv2
import os
import logging
from tqdm import tqdm  # For progress bar

# Setup logging for professional feedback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_model():
    """
    Loads the pre-trained YOLOv5 model from Ultralytics.
    """
    logging.info("Loading YOLOv5 model...")
    model = torch.hub.load('ultralytics/yolov5', 'yolov5s', verbose=False)
    return model

def process_images(model, image_folder):
    """
    Perform object detection on images in the specified folder using the YOLOv5 model.
    
    Args:
        model: Loaded YOLOv5 model.
        image_folder: Path to the folder containing images.
    
    Returns:
        List of dictionaries containing image names, images, and detection results.
    """
    detection_results = []
    
    # Loop through the images and run object detection
    for img_name in tqdm(os.listdir(image_folder), desc="Processing images"):
        img_path = os.path.join(image_folder, img_name)

        # Read the image using OpenCV
        img = cv2.imread(img_path)
        if img is None:
            logging.warning(f"Could not read image: {img_name}. Skipping.")
            continue

        # Run object detection
        results = model(img)

        # Store the results
        detection_results.append({
            'image_name': img_name,
            'image': img,
            'results': results
        })
        
        logging.info(f"Processed image: {img_name}")

    return detection_results

def save_detections(detection_results, save_dir):
    """
    Save detection results (bounding boxes, labels, confidence) to CSV files.
    
    Args:
        detection_results: List of dictionaries containing detection results.
        save_dir: Directory to save the CSV detection results.
    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    for result in detection_results:
        img_name = result['image_name']
        results = result['results']
        
        # Extract bounding boxes and other data into a DataFrame
        detections = results.pandas().xyxy[0]

        # Save detection results as CSV
        csv_path = os.path.join(save_dir, f"{img_name}_detections.csv")
        detections[['name', 'confidence', 'xmin', 'ymin', 'xmax', 'ymax']].to_csv(csv_path, index=False)
        
        logging.info(f"Saved detection results for {img_name} to {csv_path}")

def display_results(detection_results, num_images=10):
    """
    Display detection results for the first N images.
    
    Args:
        detection_results: List of dictionaries containing detection results.
        num_images: Number of images to display (default: 10).
    """
    for i in range(min(num_images, len(detection_results))):
        img_name = detection_results[i]['image_name']
        results = detection_results[i]['results']
        
        logging.info(f"Displaying results for {img_name}...")
        
        # Show the detection results on the image
        results.show()

def main(image_folder, save_dir='../data/detection_results', num_display_images=10):
    """
    Main function to perform object detection on a folder of images.
    
    Args:
        image_folder: Path to the folder containing images.
        save_dir: Directory to save the detection results (CSV).
        num_display_images: Number of images to display with detection results.
    """
    # Load the model
    model = load_model()

    # Process the images in the folder
    detection_results = process_images(model, image_folder)

    # Save the detection results as CSV
    save_detections(detection_results, save_dir)

    # Display the first N detection results
    display_results(detection_results, num_images=num_display_images)

if __name__ == '__main__':
    image_folder = '../data/photos'
    main(image_folder)
