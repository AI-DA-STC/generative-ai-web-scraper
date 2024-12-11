import copy
import json
import os

from loguru import logger

from magic_pdf.data.data_reader_writer import FileBasedDataWriter
from magic_pdf.pipe.OCRPipe import OCRPipe
from magic_pdf.pipe.TXTPipe import TXTPipe
from magic_pdf.pipe.UNIPipe import UNIPipe


def pdf_parse_main(
        pdf_path: str,
        parse_method: str = 'ocr',
        model_json_path: str = None,
        output_dir: str = None
):
    """Execute the process of converting PDF to JSON and MD formats, outputting MD and JSON files to the PDF file's directory.

    Args:
        pdf_path: Path to the .pdf file, can be relative or absolute path
        parse_method: Parsing method, three options: auto, ocr, txt. Default is auto. If results are unsatisfactory, try ocr
        model_json_path: Path to existing model data file. If empty, uses built-in model. PDF and model_json must correspond
        is_json_md_dump: Whether to write parsed data to .json and .md files. Default True. Will write different stages of data 
                        to different .json files (3 total), and MD content will be saved to .md file
        is_draw_visualization_bbox: Whether to draw visualization bounding boxes. Default True. Will generate layout and span box images
        output_dir: Output directory path. Will create a folder named after the PDF file and save all results there
    """
    try:
        pdf_name = os.path.basename(pdf_path).split('.')[0]
        pdf_path_parent = os.path.dirname(pdf_path)

        if output_dir:
            output_path = os.path.join(output_dir, pdf_name)
        else:
            output_path = os.path.join(pdf_path_parent, pdf_name)

        output_image_path = os.path.join(output_path, 'images')

        # Get parent path of images to save relative paths in .md and content_list.json files
        image_path_parent = os.path.basename(output_image_path)

        pdf_bytes = open(pdf_path, 'rb').read()  # Read PDF file's binary data

        orig_model_list = []

        if model_json_path:
            # Read original JSON data from previously model-parsed PDF file (list type)
            model_json = json.loads(open(model_json_path, 'r', encoding='utf-8').read())
            orig_model_list = copy.deepcopy(model_json)
        else:
            model_json = []

        # Execute parsing steps
        image_writer, md_writer = FileBasedDataWriter(output_image_path), FileBasedDataWriter(output_path)

        # Select parsing method
        if parse_method == 'auto':
            jso_useful_key = {'_pdf_type': '', 'model_list': model_json}
            pipe = UNIPipe(pdf_bytes, jso_useful_key, image_writer)
        elif parse_method == 'txt':
            pipe = TXTPipe(pdf_bytes, model_json, image_writer)
        elif parse_method == 'ocr':
            pipe = OCRPipe(pdf_bytes, model_json, image_writer)
        else:
            logger.error('unknown parse method, only auto, ocr, txt allowed')
            exit(1)

        # Execute classification
        pipe.pipe_classify()

        # If no model data provided, use built-in model to parse
        if len(model_json) == 0:
            pipe.pipe_analyze()  # Parse

        # Execute parsing
        pipe.pipe_parse()

        # Save results in text and md formats
        content_list = pipe.pipe_mk_uni_format(image_path_parent, drop_mode='none')
        md_content = pipe.pipe_mk_markdown(image_path_parent, drop_mode='none')

        return md_content

    except Exception as e:
        logger.exception(e)