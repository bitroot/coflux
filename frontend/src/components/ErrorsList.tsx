type Props = {
  errors: string[] | undefined;
  message?: string;
  translate: (key: string) => string;
};

export default function ErrorsList({ errors, message, translate }: Props) {
  if (errors) {
    return (
      <div className="bg-red-100 text-red-800 px-3 py-2 rounded-sm">
        {message && <p>{message}</p>}
        <ul className="list-disc ml-5">
          {errors.map((error, index) => (
            <li key={index}>{translate(error)}</li>
          ))}
        </ul>
      </div>
    );
  } else {
    return null;
  }
}
