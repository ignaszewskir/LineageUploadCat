"""Python Script to Encrypt and Decrypt Configuration Values."""

import argparse
import getpass
from src.mstrconnector.configs import ConfigEncryption


def main():

    parser = argparse.ArgumentParser(description='Encrypt and Decrypt Configuration Values')
    parser.add_argument('--method', '-m', required=True, choices=['encrypt', 'decrypt'],
                        help='encrypt or decrypt the value')
    parser.add_argument('--value', '-v', required=False, help='value to be encrypted or decrypted')

    args = parser.parse_args()
    encryption_helper = ConfigEncryption()

    if not args.value:

        while True:
            value = getpass.getpass('Configuration Value: ')
            confirm_value = getpass.getpass('Confirm Configuration Value: ')

            if confirm_value != value:
                print('Values do not match. Please try again.')
            else:
                break

    else:
        value = args.value

    if args.method == 'encrypt':
        encrypted_value = encryption_helper.encrypt_string(value)
        print(f'Encrypted Value: {encrypted_value}')

    elif args.method == 'decrypt':
        decrypted_value = encryption_helper.decrypt_string(value)
        print(f'Decrypted Value: {decrypted_value}')


if __name__ == '__main__':
    main()
