#include "udf_samples/short_encode.h"

std::string ShortEncode::encode(const unsigned char *str, int bytes)
{
    int num = 0, bin = 0, i;
    std::string _encode_result;
    const unsigned char *current;
    current = str;
    while (bytes > 2) {
        _encode_result += _base64_table[current[0] >> 2];
        _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
        _encode_result += _base64_table[((current[1] & 0x0f) << 2) + (current[2] >> 6)];
        _encode_result += _base64_table[current[2] & 0x3f];

        current += 3;
        bytes -= 3;
    }
    if (bytes > 0) {
        _encode_result += _base64_table[current[0] >> 2];
        if (bytes % 3 == 1)
        {
            _encode_result += _base64_table[(current[0] & 0x03) << 4];
            _encode_result += "==";
        }
        else if (bytes % 3 == 2)
        {
            _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
            _encode_result += _base64_table[(current[1] & 0x0f) << 2];
            _encode_result += "=";
        }
    }
    return _encode_result;
}

short Encode::decode(const char *data, size_t length, char *decoded_data)
{
    const char *current = data;
    char decoded_date[8] = "";
    int ch = 0, i = 0, j = 0, k = 0;

    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decode_table[ch];
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
        case 0:
            decoded_data[j] = ch << 2;
            break;
        case 1:
            decoded_data[j++] |= ch >> 4;
            decoded_data[j] = (ch & 0x0f) << 4;
            break;
        case 2:
            decoded_data[j++] |= ch >> 2;
            decoded_data[j] = (ch & 0x03) << 6;
            break;
        case 3:
            decoded_data[j++] |= ch;
            break;
        default:
            break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
        case 1:
            return 0;
        case 2:
            k++;
        case 3:
            decoded_data[k] = 0;
        default:
            break;
        }
    }

    decoded_data[j] = '\0';
    return *((short *)decoded_data);
}