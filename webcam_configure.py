#!/usr/bin/env python2

# Erik Husby, 2018


from numbers import Number

import cv2
import keyboard


global CAM
CAM = cv2.VideoCapture(0)

global PROP_INFO_LIST
PROP_INFO_STR = """\
CV_CAP_PROP_POS_MSEC: Current position of the video file in milliseconds or video capture timestamp.
CV_CAP_PROP_POS_FRAMES: 0-based index of the frame to be decoded/captured next.
CV_CAP_PROP_POS_AVI_RATIO: Relative position of the video file: 0 - start of the film, 1 - end of the film.
CV_CAP_PROP_FRAME_WIDTH: Width of the frames in the video stream.
CV_CAP_PROP_FRAME_HEIGHT: Height of the frames in the video stream.
CV_CAP_PROP_FPS: Frame rate.
CV_CAP_PROP_FOURCC: 4-character code of codec.
CV_CAP_PROP_FRAME_COUNT: Number of frames in the video file.
CV_CAP_PROP_FORMAT: Format of the Mat objects returned by retrieve().
CV_CAP_PROP_MODE: Backend-specific value indicating the current capture mode.
CV_CAP_PROP_BRIGHTNESS: Brightness of the image (only for cameras).
CV_CAP_PROP_CONTRAST: Contrast of the image (only for cameras).
CV_CAP_PROP_SATURATION: Saturation of the image (only for cameras).
CV_CAP_PROP_HUE: Hue of the image (only for cameras).
CV_CAP_PROP_GAIN: Gain of the image (only for cameras).
CV_CAP_PROP_EXPOSURE: Exposure (only for cameras).
CV_CAP_PROP_CONVERT_RGB: Boolean flags indicating whether images should be converted to RGB.
CV_CAP_PROP_WHITE_BALANCE_U: The U value of the whitebalance setting (note: only supported by DC1394 v 2.x backend currently)
CV_CAP_PROP_WHITE_BALANCE_V: The V value of the whitebalance setting (note: only supported by DC1394 v 2.x backend currently)
CV_CAP_PROP_RECTIFICATION: Rectification flag for stereo cameras (note: only supported by DC1394 v 2.x backend currently)
CV_CAP_PROP_ISO_SPEED: The ISO speed of the camera (note: only supported by DC1394 v 2.x backend currently)
CV_CAP_PROP_BUFFERSIZE: Amount of frames stored in internal buffer memory (note: only supported by DC1394 v 2.x backend currently)
"""
PROP_INFO_LIST = PROP_INFO_STR.splitlines()

global PROP_NUM
PROP_NUM = None

global PROP_TYPE
PROP_TYPE = None

global INC_VAL
INC_VAL = None


def main():
    global PROP_INFO_LIST
    print "--- VIDEO CAPTURE PROPERTIES ---"
    for i, prop_descrip in enumerate(PROP_INFO_LIST):
        print "[{:2}] {}".format(i, prop_descrip)
    print

    print "Press [SHIFT] to select a property to modify"
    print "Press [ALT] to set property value"
    print "Press [CTRL] to set increment value to be used with [LEFT] and [RIGHT] arrow keys"

    keyboard.add_hotkey('shift', lambda: set_prop_num())
    keyboard.add_hotkey('alt', lambda: set_prop_val())
    keyboard.add_hotkey('space', lambda: print_prop_val())
    keyboard.add_hotkey('ctrl', lambda: set_inc_val())
    keyboard.add_hotkey('right', lambda: inc_prop(1))
    keyboard.add_hotkey('left', lambda: inc_prop(-1))

    show_webcam(mirror=True)


def show_webcam(mirror=False):
    # https://gist.github.com/tedmiston/6060034
    global CAM
    while True:
        ret_val, img = CAM.read()
        if mirror:
            img = cv2.flip(img, 1)
        cv2.imshow('USB Webcam', img)
        if cv2.waitKey(1) == 27:
            break  # Press Escape to quit.
    cv2.destroyAllWindows()


def set_prop_num():
    global CAM, PROP_INFO_LIST, PROP_NUM, PROP_TYPE, INC_VAL

    print
    while True:
        prop_num_new = input("Switch to camera property index #: ")
        try:
            prop_num_new = int(prop_num_new)
            if 0 <= prop_num_new <= 21:
                pass
            else:
                raise ValueError
            break
        except ValueError:
            print "Invalid input"

    PROP_NUM = prop_num_new
    PROP_TYPE = type(CAM.get(PROP_NUM))
    INC_VAL = None

    print PROP_INFO_LIST[PROP_NUM]
    print "*** CURRENT VALUE: '{}' ***".format(CAM.get(PROP_NUM))
    print "*** TYPE OF VALUE: {} ***".format(PROP_TYPE)


def set_prop_val():
    global PROP_NUM, PROP_TYPE
    if PROP_NUM is None:
        print "Press [SHIFT] to select a property to modify"
        return

    while True:
        prop_val_new = input("Enter new value for selected property: ")
        try:
            if prop_val_new == '':
                return
            prop_val_new = PROP_TYPE(prop_val_new)
            break
        except ValueError:
            print "Invalid input"

    CAM.set(PROP_NUM, prop_val_new)


def print_prop_val():
    global CAM, PROP_NUM
    print "Property value = {}".format(CAM.get(PROP_NUM))


def set_inc_val():
    global PROP_NUM, INC_VAL
    if PROP_NUM is None:
        print "Press [SHIFT] to select a property to modify"
        return
    if not isinstance(CAM.get(PROP_NUM), Number):
        print "This property cannot be incremented"
        return

    while True:
        inc_val_new = input("Enter new increment value: ")
        try:
            if inc_val_new == '':
                return
            inc_val_new = float(inc_val_new)
            break
        except ValueError:
            print "Invalid input"

    INC_VAL = inc_val_new


def inc_prop(direction):
    global CAM, PROP_NUM, INC_VAL
    if PROP_NUM is None:
        print "Press [SHIFT] to select a property to modify"
        return
    if INC_VAL is None:
        print "Press [CTRL] to set increment value"
        return
    if not isinstance(CAM.get(PROP_NUM), Number):
        print "ERROR: This property cannot be incremented"
        return

    CAM.set(PROP_NUM, CAM.get(PROP_NUM) + direction*INC_VAL)



if __name__ == '__main__':
    main()
